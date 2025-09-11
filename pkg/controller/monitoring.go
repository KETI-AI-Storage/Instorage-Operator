package controller

import (
	"context"
	"fmt"
	instoragev1alpha1 "instorage-operator/pkg/apis/v1alpha1"
	pb "instorage-operator/pkg/proto"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// monitorCSDJob monitors a job running on CSD via Instorage Manager
func (r *InstorageJobReconciler) monitorCSDJob(ctx context.Context, job *instoragev1alpha1.InstorageJob) (ctrl.Result, error) {
	r.Log.Info("Monitoring CSD job via Instorage Manager", "job", job.Name, "node", job.Status.TargetNode)

	// Get job status from Instorage Manager
	status, err := r.getJobStatusFromManager(ctx, job.Status.TargetNode, job.Name)
	if err != nil {
		r.Log.Error(err, "Failed to get job status from manager", "job", job.Name)
		// Don't fail immediately, retry
		return ctrl.Result{RequeueAfter: time.Second * DefaultRequeueInterval}, nil
	}

	// Update job status based on manager response
	switch status.Status {
	case pb.JobStatus_JOB_STATUS_COMPLETED:
		job.Status.Phase = instoragev1alpha1.JobPhaseSucceeded
		job.Status.CompletionTime = &metav1.Time{Time: time.Now()}
		job.Status.Message = "CSD job completed successfully"

		// Update batch execution status if available
		if status.BatchExecution != nil {
			job.Status.BatchExecution = &instoragev1alpha1.BatchExecutionStatus{
				TotalBatches:     int(status.BatchExecution.TotalBatches),
				CompletedBatches: int(status.BatchExecution.CompletedBatches),
				FailedBatches:    int(status.BatchExecution.FailedBatches),
				RunningBatches:   int(status.BatchExecution.RunningBatches),
			}
		}

		condition := instoragev1alpha1.JobCondition{
			Type:               "Completed",
			Status:             "True",
			LastTransitionTime: metav1.Now(),
			Reason:             "CSDJobSucceeded",
			Message:            "CSD job completed successfully",
		}
		job.Status.Conditions = append(job.Status.Conditions, condition)
		return ctrl.Result{}, r.Status().Update(ctx, job)

	case pb.JobStatus_JOB_STATUS_FAILED:
		job.Status.Phase = instoragev1alpha1.JobPhaseFailed
		job.Status.CompletionTime = &metav1.Time{Time: time.Now()}
		job.Status.Message = fmt.Sprintf("CSD job failed: %s", status.Message)

		condition := instoragev1alpha1.JobCondition{
			Type:               "Failed",
			Status:             "True",
			LastTransitionTime: metav1.Now(),
			Reason:             "CSDJobFailed",
			Message:            status.Message,
		}
		job.Status.Conditions = append(job.Status.Conditions, condition)
		return ctrl.Result{}, r.Status().Update(ctx, job)

	case pb.JobStatus_JOB_STATUS_RUNNING:
		// Update progress if batch execution info is available
		if status.BatchExecution != nil {
			if job.Status.BatchExecution == nil {
				job.Status.BatchExecution = &instoragev1alpha1.BatchExecutionStatus{}
			}
			job.Status.BatchExecution.TotalBatches = int(status.BatchExecution.TotalBatches)
			job.Status.BatchExecution.CompletedBatches = int(status.BatchExecution.CompletedBatches)
			job.Status.BatchExecution.FailedBatches = int(status.BatchExecution.FailedBatches)
			job.Status.BatchExecution.RunningBatches = int(status.BatchExecution.RunningBatches)

			progress := float64(status.BatchExecution.CompletedBatches+status.BatchExecution.FailedBatches) /
				float64(status.BatchExecution.TotalBatches) * 100
			job.Status.Message = fmt.Sprintf("CSD job running: %.1f%% complete (%d/%d batches)",
				progress,
				status.BatchExecution.CompletedBatches+status.BatchExecution.FailedBatches,
				status.BatchExecution.TotalBatches)
		} else {
			job.Status.Message = "CSD job is running"
		}

		if err := r.Status().Update(ctx, job); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: time.Second * DefaultRequeueInterval}, nil

	default:
		r.Log.Info("Unknown job status from manager", "status", status.Status)
		return ctrl.Result{RequeueAfter: time.Second * DefaultRequeueInterval}, nil
	}
}

// monitorBatchJobs monitors multiple Kubernetes batch jobs
func (r *InstorageJobReconciler) monitorBatchJobs(ctx context.Context, job *instoragev1alpha1.InstorageJob) (ctrl.Result, error) {
	r.Log.Info("Monitoring batch Kubernetes jobs", "job", job.Name,
		"totalBatches", job.Status.BatchExecution.TotalBatches)

	// List all batch jobs for this parent job
	batchJobList := &batchv1.JobList{}
	labelSelector := client.MatchingLabels{
		"instorage.job.parent": job.Name,
		"instorage.job.type":   "batch",
	}

	if err := r.List(ctx, batchJobList, labelSelector); err != nil {
		r.Log.Error(err, "Failed to list batch jobs")
		return ctrl.Result{}, err
	}

	// Count job statuses
	var running, succeeded, failed int32
	completedBatchIds := make(map[string]bool)
	failedBatchIds := make(map[string]bool)

	for _, batchJob := range batchJobList.Items {
		batchId := batchJob.Labels["instorage.batch.id"]

		if batchJob.Status.Succeeded > 0 {
			succeeded++
			completedBatchIds[batchId] = true
		} else if batchJob.Status.Failed > 0 {
			failed++
			failedBatchIds[batchId] = true
		} else if batchJob.Status.Active > 0 {
			running++
		}
	}

	// Update batch execution status
	job.Status.BatchExecution.CompletedBatches = int(succeeded)
	job.Status.BatchExecution.FailedBatches = int(failed)
	job.Status.BatchExecution.RunningBatches = int(running)

	r.Log.Info("Batch job status",
		"completed", succeeded,
		"failed", failed,
		"running", running,
		"total", job.Status.BatchExecution.TotalBatches)

	// Check if we need to create more batch jobs
	totalProcessed := succeeded + failed + running
	if totalProcessed < int32(job.Status.BatchExecution.TotalBatches) {
		// Get batch plan to find remaining batches
		batchPlan, err := r.getBatchPlanFromAnnotations(job)
		if err != nil {
			r.Log.Error(err, "Failed to get batch plan")
		} else {
			// Calculate how many more we can start
			maxParallel := batchPlan.MaxParallelJobs
			if maxParallel <= 0 {
				maxParallel = DefaultMaxParallelJobs
			}

			canStart := maxParallel - running
			if canStart > 0 {
				r.Log.Info("Creating additional batch jobs",
					"canStart", canStart,
					"running", running,
					"maxParallel", maxParallel)

				// Find and create unprocessed batches
				for i, batch := range batchPlan.Batches {
					if completedBatchIds[batch.BatchId] || failedBatchIds[batch.BatchId] {
						continue // Already processed
					}

					// Check if this batch job already exists
					exists := false
					for _, bj := range batchJobList.Items {
						if bj.Labels["instorage.batch.id"] == batch.BatchId {
							exists = true
							break
						}
					}

					if !exists && canStart > 0 {
						batchJob, err := r.createBatchKubernetesJob(ctx, job, batch, i)
						if err != nil {
							r.Log.Error(err, "Failed to create batch job", "batchId", batch.BatchId)
							continue
						}

						if err := controllerutil.SetControllerReference(job, batchJob, r.Scheme); err != nil {
							r.Log.Error(err, "Failed to set controller reference", "batchId", batch.BatchId)
							continue
						}

						if err := r.Create(ctx, batchJob); err != nil {
							r.Log.Error(err, "Failed to create batch job", "batchId", batch.BatchId)
							job.Status.BatchExecution.FailedBatches++
							continue
						}

						r.Log.Info("Created additional batch job",
							"batchId", batch.BatchId,
							"batchJob", batchJob.Name)

						job.Status.BatchExecution.RunningBatches++
						canStart--
					}
				}
			}
		}
	}

	// Check if all batches are complete
	totalComplete := succeeded + failed
	if totalComplete >= int32(job.Status.BatchExecution.TotalBatches) {
		if failed > 0 {
			job.Status.Phase = instoragev1alpha1.JobPhaseFailed
			job.Status.Message = fmt.Sprintf("Batch job failed: %d/%d batches failed",
				failed, job.Status.BatchExecution.TotalBatches)
		} else {
			job.Status.Phase = instoragev1alpha1.JobPhaseSucceeded
			job.Status.Message = fmt.Sprintf("All %d batches completed successfully",
				job.Status.BatchExecution.TotalBatches)
		}
		job.Status.CompletionTime = &metav1.Time{Time: time.Now()}

		condition := instoragev1alpha1.JobCondition{
			Type:               "BatchJobsComplete",
			Status:             "True",
			LastTransitionTime: metav1.Now(),
			Reason:             "AllBatchesProcessed",
			Message:            fmt.Sprintf("Completed: %d, Failed: %d", succeeded, failed),
		}
		job.Status.Conditions = append(job.Status.Conditions, condition)

		return ctrl.Result{}, r.Status().Update(ctx, job)
	}

	// Update progress message
	progress := float64(totalComplete) / float64(job.Status.BatchExecution.TotalBatches) * 100
	job.Status.Message = fmt.Sprintf("Batch job running: %.1f%% complete (%d/%d batches)",
		progress, totalComplete, job.Status.BatchExecution.TotalBatches)

	if err := r.Status().Update(ctx, job); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: time.Second * 10}, nil
}

// monitorSingleKubernetesJob monitors a single Kubernetes job (existing logic)
func (r *InstorageJobReconciler) monitorSingleKubernetesJob(ctx context.Context, job *instoragev1alpha1.InstorageJob) (ctrl.Result, error) {
	r.Log.Info("Monitoring single Kubernetes job", "job", job.Name)

	k8sJob := &batchv1.Job{}
	err := r.Get(ctx, types.NamespacedName{
		Name:      job.Name,
		Namespace: job.Namespace,
	}, k8sJob)

	if err != nil {
		if apierrors.IsNotFound(err) {
			job.Status.Phase = instoragev1alpha1.JobPhaseFailed
			job.Status.Message = "Kubernetes Job not found"
			return ctrl.Result{}, r.Status().Update(ctx, job)
		}
		return ctrl.Result{}, err
	}

	if k8sJob.Status.Succeeded > 0 {
		job.Status.Phase = instoragev1alpha1.JobPhaseSucceeded
		job.Status.CompletionTime = &metav1.Time{Time: time.Now()}
		job.Status.Message = "Job completed successfully"

		condition := instoragev1alpha1.JobCondition{
			Type:               "Completed",
			Status:             "True",
			LastTransitionTime: metav1.Now(),
			Reason:             "JobSucceeded",
			Message:            "Job completed successfully",
		}
		job.Status.Conditions = append(job.Status.Conditions, condition)

		return ctrl.Result{}, r.Status().Update(ctx, job)
	}

	if k8sJob.Status.Failed > 0 {
		job.Status.Phase = instoragev1alpha1.JobPhaseFailed
		job.Status.CompletionTime = &metav1.Time{Time: time.Now()}
		job.Status.Message = "Job failed"

		condition := instoragev1alpha1.JobCondition{
			Type:               "Failed",
			Status:             "True",
			LastTransitionTime: metav1.Now(),
			Reason:             "JobFailed",
			Message:            "Job execution failed",
		}
		job.Status.Conditions = append(job.Status.Conditions, condition)

		return ctrl.Result{}, r.Status().Update(ctx, job)
	}

	return ctrl.Result{RequeueAfter: time.Second * LongRequeueInterval}, nil
}

// getJobStatusFromManager queries job status from Instorage Manager via gRPC
func (r *InstorageJobReconciler) getJobStatusFromManager(ctx context.Context, nodeName, jobName string) (*pb.GetJobStatusResponse, error) {
	// Build gRPC server address
	serverAddr := fmt.Sprintf("%s:%s", nodeName, DefaultInstorageManagerPort)

	// Create gRPC connection
	conn, err := grpc.DialContext(ctx, serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to instorage manager at %s: %w", serverAddr, err)
	}
	defer conn.Close()

	// Create gRPC client
	client := pb.NewInstorageManagerClient(conn)

	// Query job status
	request := &pb.GetJobStatusRequest{
		JobId: jobName,
	}

	response, err := client.GetJobStatus(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("failed to get job status from instorage manager: %w", err)
	}

	return response, nil
}

// getBatchPlanFromAnnotations retrieves stored batch plan from job annotations
func (r *InstorageJobReconciler) getBatchPlanFromAnnotations(job *instoragev1alpha1.InstorageJob) (*pb.BatchPlan, error) {
	// For now, return a mock batch plan
	// In a real implementation, you would store the batch plan in annotations during creation
	// and retrieve it here

	if job.Status.BatchExecution == nil {
		return nil, fmt.Errorf("no batch execution status found")
	}

	totalBatches := job.Status.BatchExecution.TotalBatches
	if totalBatches <= 0 {
		return nil, fmt.Errorf("invalid total batches: %d", totalBatches)
	}

	// Create a mock batch plan based on stored status
	batches := make([]*pb.BatchInfo, totalBatches)
	for i := 0; i < totalBatches; i++ {
		batches[i] = &pb.BatchInfo{
			BatchId:            fmt.Sprintf("batch-%d", i),
			ItemCount:          DefaultBatchItemCount,                      // mock value
			EstimatedSizeBytes: DefaultBatchSizeBytes,                      // mock value
			FilePaths:          []string{fmt.Sprintf("/data/batch-%d", i)}, // mock value
		}
	}

	batchPlan := &pb.BatchPlan{
		TotalBatches:    int32(totalBatches),
		MaxParallelJobs: DefaultMaxParallelJobs, // default value, should be stored in annotations
		Strategy:        "auto",                 // default value, should be stored in annotations
		Batches:         batches,
	}

	return batchPlan, nil
}
