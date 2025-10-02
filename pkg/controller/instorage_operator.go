package controller

import (
	"context"
	"fmt"
	"time"

	instoragev1alpha1 "instorage-operator/pkg/apis/v1alpha1"
	pb "instorage-operator/pkg/proto"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	InstorageJobFinalizer       = "instorage.batch.csd.io/finalizer"
	DefaultInstorageManagerPort = "50051"

	// Requeue intervals (seconds)
	DefaultRequeueInterval = 10
	LongRequeueInterval    = 30

	// Batch processing defaults
	DefaultMaxParallelJobs = 5
	DefaultBatchTTL        = 3600 // seconds
	DefaultBatchItemCount  = 100
	DefaultBatchSizeBytes  = 1024 * 1024 // 1MB

	// Batch execution init values
	DefaultBatchCompletedInit = 0
	DefaultBatchFailedInit    = 0
	DefaultBatchRunningInit   = 0

	// Batch configuration defaults from build_submit_job.go
	DefaultMaxBatchSizeSmall  = 100
	DefaultMaxBatchSizeMedium = 200
	DefaultMaxBatchSizeLarge  = 500
	DefaultMaxBatchSizeHuge   = 2000
	DefaultMaxBatchSizeTiny   = 10

	DefaultMaxParallelSmall  = 5
	DefaultMaxParallelMedium = 10
	DefaultMaxParallelLarge  = 20
	DefaultMaxParallelHuge   = 50
	DefaultMaxParallelTiny   = 1

	DefaultTotalItems = 1000

	// Kubernetes Job defaults
	DefaultParallelism             = 1
	DefaultCompletions             = 1
	DefaultBackoffLimit            = 3
	DefaultTTLSecondsAfterFinished = 3600

	// Validation limits
	MaxImageNameLength = 255
	MaxBatchSizeLimit  = 10000
)

// InstorageJobReconciler reconciles an InstorageJob object
type InstorageJobReconciler struct {
	client.Client
	Log    logr.Logger
	Scheme *runtime.Scheme
}

func (r *InstorageJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&instoragev1alpha1.InstorageJob{}).
		Complete(r)
}

func (r *InstorageJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("instoragejob", req.NamespacedName)

	job := &instoragev1alpha1.InstorageJob{}
	err := r.Get(ctx, req.NamespacedName, job)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("InstorageJob resource not found")
			return ctrl.Result{}, nil
		}
		log.Error(err, "Failed to get InstorageJob")
		return ctrl.Result{}, err
	}

	if job.ObjectMeta.DeletionTimestamp.IsZero() {
		if !controllerutil.ContainsFinalizer(job, InstorageJobFinalizer) {
			controllerutil.AddFinalizer(job, InstorageJobFinalizer)
			return ctrl.Result{}, r.Update(ctx, job)
		}
	} else {
		if controllerutil.ContainsFinalizer(job, InstorageJobFinalizer) {
			if err := r.cleanup(ctx, job); err != nil {
				return ctrl.Result{}, err
			}
			controllerutil.RemoveFinalizer(job, InstorageJobFinalizer)
			return ctrl.Result{}, r.Update(ctx, job)
		}
		return ctrl.Result{}, nil
	}

	switch job.Status.Phase {
	case "":
		return r.handleNewJob(ctx, job)
	case instoragev1alpha1.JobPhasePending:
		return r.handlePendingJob(ctx, job)
	case instoragev1alpha1.JobPhaseRunning:
		return r.handleRunningJob(ctx, job)
	case instoragev1alpha1.JobPhaseSucceeded, instoragev1alpha1.JobPhaseFailed:
		return ctrl.Result{}, nil
	default:
		log.Info("Unknown job phase", "phase", job.Status.Phase)
		return ctrl.Result{}, nil
	}
}

func (r *InstorageJobReconciler) handleNewJob(ctx context.Context, job *instoragev1alpha1.InstorageJob) (ctrl.Result, error) {
	r.Log.Info("Handling new job", "job", job.Name)

	job.Status.Phase = instoragev1alpha1.JobPhasePending
	job.Status.StartTime = &metav1.Time{Time: time.Now()}
	job.Status.Message = "Job created and waiting for execution"

	condition := instoragev1alpha1.JobCondition{
		Type:               "Initialized",
		Status:             "True",
		LastTransitionTime: metav1.Now(),
		Reason:             "JobCreated",
		Message:            "InstorageJob has been created",
	}
	job.Status.Conditions = append(job.Status.Conditions, condition)

	return ctrl.Result{}, r.Status().Update(ctx, job)
}

func (r *InstorageJobReconciler) handlePendingJob(ctx context.Context, job *instoragev1alpha1.InstorageJob) (ctrl.Result, error) {
	r.Log.Info("Handling pending job", "job", job.Name)

	if err := r.validateJobSpec(job); err != nil {
		r.Log.Error(err, "Job validation failed", "job", job.Name)
		job.Status.Phase = instoragev1alpha1.JobPhaseFailed
		job.Status.Message = fmt.Sprintf("Validation failed: %v", err)
		return ctrl.Result{}, r.Status().Update(ctx, job)
	}

	// CSD 활성화 여부에 따라 다른 처리 방식 선택
	if job.Spec.CSD != nil && job.Spec.CSD.Enabled {
		r.Log.Info("CSD enabled - submitting to CSD manager", "job", job.Name)
		return r.handleCSDJob(ctx, job)
	} else {
		r.Log.Info("CSD disabled - creating Kubernetes Job", "job", job.Name)
		return r.handleKubernetesJob(ctx, job)
	}
}

// handleCSDJob - CSD Manager에게 작업 제출
func (r *InstorageJobReconciler) handleCSDJob(ctx context.Context, job *instoragev1alpha1.InstorageJob) (ctrl.Result, error) {
	r.Log.Info("Submitting job to CSD Manager", "job", job.Name)

	// 1. CSD 노드 선택
	targetNode, err := r.selectCSDNode(job)
	if err != nil {
		job.Status.Phase = instoragev1alpha1.JobPhaseFailed
		job.Status.Message = fmt.Sprintf("Failed to select CSD node: %v", err)
		return ctrl.Result{}, r.Status().Update(ctx, job)
	}

	// 2. Instorage Manager에게 작업 제출
	if err := r.submitToInstorageManager(ctx, targetNode, job); err != nil {
		job.Status.Phase = instoragev1alpha1.JobPhaseFailed
		job.Status.Message = fmt.Sprintf("Failed to submit to CSD manager: %v", err)
		return ctrl.Result{}, r.Status().Update(ctx, job)
	}

	// 3. 상태 업데이트
	job.Status.Phase = instoragev1alpha1.JobPhaseRunning
	job.Status.Message = fmt.Sprintf("Job submitted to CSD node: %s", targetNode)
	job.Status.TargetNode = targetNode

	condition := instoragev1alpha1.JobCondition{
		Type:               "CSDJobSubmitted",
		Status:             "True",
		LastTransitionTime: metav1.Now(),
		Reason:             "SubmittedToCSD",
		Message:            fmt.Sprintf("Job submitted to CSD manager on node %s", targetNode),
	}
	job.Status.Conditions = append(job.Status.Conditions, condition)

	return ctrl.Result{}, r.Status().Update(ctx, job)
}

// handleKubernetesJob - Kubernetes Job 생성 방식 (배치 처리 지원)
func (r *InstorageJobReconciler) handleKubernetesJob(ctx context.Context, job *instoragev1alpha1.InstorageJob) (ctrl.Result, error) {
	r.Log.Info("Creating Kubernetes Job", "job", job.Name)

	// 배치 처리가 필요한지 확인
	if r.shouldUseBatchProcessing(job) {
		return r.handleBatchKubernetesJob(ctx, job)
	}

	// 단일 작업 처리 (기존 로직)
	k8sJob, err := r.createKubernetesJob(ctx, job)
	if err != nil {
		job.Status.Phase = instoragev1alpha1.JobPhaseFailed
		job.Status.Message = fmt.Sprintf("Failed to create Kubernetes job: %v", err)
		return ctrl.Result{}, r.Status().Update(ctx, job)
	}

	if err := controllerutil.SetControllerReference(job, k8sJob, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}

	if err := r.Create(ctx, k8sJob); err != nil {
		job.Status.Phase = instoragev1alpha1.JobPhaseFailed
		job.Status.Message = fmt.Sprintf("Failed to create job: %v", err)
		return ctrl.Result{}, r.Status().Update(ctx, job)
	}

	job.Status.Phase = instoragev1alpha1.JobPhaseRunning
	job.Status.Message = "Kubernetes Job created and running"

	condition := instoragev1alpha1.JobCondition{
		Type:               "K8sJobCreated",
		Status:             "True",
		LastTransitionTime: metav1.Now(),
		Reason:             "JobCreated",
		Message:            "Kubernetes Job has been created successfully",
	}
	job.Status.Conditions = append(job.Status.Conditions, condition)

	return ctrl.Result{}, r.Status().Update(ctx, job)
}

// handleBatchKubernetesJob creates multiple Kubernetes Jobs for batch processing
func (r *InstorageJobReconciler) handleBatchKubernetesJob(ctx context.Context, job *instoragev1alpha1.InstorageJob) (ctrl.Result, error) {
	r.Log.Info("Creating batch Kubernetes Jobs", "job", job.Name)

	// 배치 계획 생성
	batchPlan, err := r.createBatchPlan(job)
	if err != nil {
		job.Status.Phase = instoragev1alpha1.JobPhaseFailed
		job.Status.Message = fmt.Sprintf("Failed to create batch plan: %v", err)
		return ctrl.Result{}, r.Status().Update(ctx, job)
	}

	r.Log.Info("Created batch plan",
		"job", job.Name,
		"totalBatches", len(batchPlan.Batches),
		"maxParallel", batchPlan.MaxParallelJobs,
	)

	// 배치 실행 상태 초기화
	if job.Status.BatchExecution == nil {
		job.Status.BatchExecution = &instoragev1alpha1.BatchExecutionStatus{}
	}
	job.Status.BatchExecution.TotalBatches = len(batchPlan.Batches)
	job.Status.BatchExecution.CompletedBatches = DefaultBatchCompletedInit
	job.Status.BatchExecution.FailedBatches = DefaultBatchFailedInit
	job.Status.BatchExecution.RunningBatches = DefaultBatchRunningInit
	job.Status.BatchExecution.BatchStrategy = batchPlan.Strategy

	// 각 배치에 대해 Kubernetes Job 생성
	maxParallel := int(batchPlan.MaxParallelJobs)
	if maxParallel <= 0 {
		maxParallel = DefaultMaxParallelJobs
	}

	createdJobs := 0
	for i, batch := range batchPlan.Batches {
		// 병렬 제한 확인 (처음 몇개만 시작)
		if i >= maxParallel {
			r.Log.Info("Reached max parallel jobs limit, will create remaining jobs later",
				"created", createdJobs,
				"total", len(batchPlan.Batches),
				"maxParallel", maxParallel,
			)
			break
		}

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

		createdJobs++
		job.Status.BatchExecution.RunningBatches++

		r.Log.Info("Created batch job",
			"parentJob", job.Name,
			"batchId", batch.BatchId,
			"batchJob", batchJob.Name,
		)
	}

	job.Status.Phase = instoragev1alpha1.JobPhaseRunning
	job.Status.Message = fmt.Sprintf("Created %d batch jobs (total: %d batches)",
		createdJobs, len(batchPlan.Batches))

	condition := instoragev1alpha1.JobCondition{
		Type:               "BatchJobsCreated",
		Status:             "True",
		LastTransitionTime: metav1.Now(),
		Reason:             "BatchJobsCreated",
		Message:            fmt.Sprintf("Created %d batch Kubernetes Jobs", createdJobs),
	}
	job.Status.Conditions = append(job.Status.Conditions, condition)

	return ctrl.Result{RequeueAfter: time.Second * DefaultRequeueInterval}, r.Status().Update(ctx, job)
}

func (r *InstorageJobReconciler) handleRunningJob(ctx context.Context, job *instoragev1alpha1.InstorageJob) (ctrl.Result, error) {
	r.Log.Info("Monitoring running job", "job", job.Name, "targetNode", job.Status.TargetNode)

	// Check job type and handle accordingly
	if job.Status.TargetNode != "" {
		// CSD job - monitor via Instorage Manager
		return r.monitorCSDJob(ctx, job)
	} else if job.Status.BatchExecution != nil && job.Status.BatchExecution.TotalBatches > 0 {
		// Batch Kubernetes jobs - monitor all batch jobs
		return r.monitorBatchJobs(ctx, job)
	} else {
		// Single Kubernetes job - existing logic
		return r.monitorSingleKubernetesJob(ctx, job)
	}
}

// Submit job to Instorage Manager via gRPC
func (r *InstorageJobReconciler) submitToInstorageManager(ctx context.Context, nodeName string, job *instoragev1alpha1.InstorageJob) error {
	r.Log.Info("Submitting job to Instorage Manager via gRPC", "node", nodeName, "job", job.Name)

	// Validate job specification before submission
	if err := r.validateJobSpec(job); err != nil {
		r.Log.Error(err, "Job validation failed", "job", job.Name)
		return fmt.Errorf("job validation failed: %w", err)
	}

	// Build gRPC server address
	serverAddr := fmt.Sprintf("%s:%s", nodeName, DefaultInstorageManagerPort)

	// Create gRPC connection
	conn, err := grpc.DialContext(ctx, serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to instorage manager at %s: %w", serverAddr, err)
	}
	defer conn.Close()

	// Create gRPC client
	client := pb.NewInstorageManagerClient(conn)

	// Build submit job request
	request := r.buildSubmitJobRequest(job, nodeName)

	// Submit job via gRPC
	response, err := client.SubmitJob(ctx, request)
	if err != nil {
		return fmt.Errorf("failed to submit job to instorage manager: %w", err)
	}

	if !response.Success {
		return fmt.Errorf("instorage manager rejected job: %s", response.Message)
	}

	r.Log.Info("Job successfully submitted to Instorage Manager",
		"job", job.Name,
		"jobId", response.JobId,
		"message", response.Message,
		"submittedAt", response.SubmittedAt.AsTime(),
	)

	return nil
}

func (r *InstorageJobReconciler) cleanup(ctx context.Context, job *instoragev1alpha1.InstorageJob) error {
	r.Log.Info("Cleaning up job", "job", job.Name)

	k8sJob := &batchv1.Job{}
	err := r.Get(ctx, types.NamespacedName{
		Name:      job.Name,
		Namespace: job.Namespace,
	}, k8sJob)

	if err == nil {
		if err := r.Delete(ctx, k8sJob); err != nil {
			return err
		}
	}

	return nil
}
