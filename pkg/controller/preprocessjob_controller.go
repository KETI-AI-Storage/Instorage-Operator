package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	instoragev1alpha1 "instorage-operator/pkg/apis/v1alpha1"
	pb "instorage-operator/pkg/proto"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	InstorageJobFinalizer       = "instorage.batch.csd.io/finalizer"
	DefaultInstorageManagerPort = "50051"
)

// InstorageJobReconciler reconciles an InstorageJob object
type InstorageJobReconciler struct {
	client.Client
	Log    logr.Logger
	Scheme *runtime.Scheme
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

	// 2. CSD Manager에게 작업 제출
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

// handleKubernetesJob - 기존 Kubernetes Job 생성 방식
func (r *InstorageJobReconciler) handleKubernetesJob(ctx context.Context, job *instoragev1alpha1.InstorageJob) (ctrl.Result, error) {
	r.Log.Info("Creating Kubernetes Job", "job", job.Name)

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

func (r *InstorageJobReconciler) handleRunningJob(ctx context.Context, job *instoragev1alpha1.InstorageJob) (ctrl.Result, error) {
	r.Log.Info("Monitoring running job", "job", job.Name)

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

	return ctrl.Result{RequeueAfter: time.Second * 30}, nil
}

func (r *InstorageJobReconciler) validateJobSpec(job *instoragev1alpha1.InstorageJob) error {
	if job.Spec.Image == "" {
		return fmt.Errorf("image is required")
	}
	if job.Spec.DataPath == "" {
		return fmt.Errorf("dataPath is required")
	}
	if job.Spec.OutputPath == "" {
		return fmt.Errorf("outputPath is required")
	}
	return nil
}

func (r *InstorageJobReconciler) createKubernetesJob(ctx context.Context, job *instoragev1alpha1.InstorageJob) (*batchv1.Job, error) {
	jobSpec := r.buildJobSpec(job)

	k8sJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      job.Name,
			Namespace: job.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":       "instorage-job",
				"app.kubernetes.io/instance":   job.Name,
				"app.kubernetes.io/managed-by": "instorage-operator",
			},
		},
		Spec: *jobSpec,
	}

	return k8sJob, nil
}

func (r *InstorageJobReconciler) buildJobSpec(job *instoragev1alpha1.InstorageJob) *batchv1.JobSpec {
	parallelism := int32(1)
	completions := int32(1)
	backoffLimit := int32(3)
	ttlSecondsAfterFinished := int32(3600)

	if job.Spec.JobConfig != nil {
		if job.Spec.JobConfig.Parallelism > 0 {
			parallelism = int32(job.Spec.JobConfig.Parallelism)
		}
		if job.Spec.JobConfig.Completions > 0 {
			completions = int32(job.Spec.JobConfig.Completions)
		}
		if job.Spec.JobConfig.BackoffLimit > 0 {
			backoffLimit = int32(job.Spec.JobConfig.BackoffLimit)
		}
		if job.Spec.JobConfig.TTLSecondsAfterFinished > 0 {
			ttlSecondsAfterFinished = int32(job.Spec.JobConfig.TTLSecondsAfterFinished)
		}
	}

	podSpec := r.buildPodSpec(job)

	return &batchv1.JobSpec{
		Parallelism:             &parallelism,
		Completions:             &completions,
		BackoffLimit:            &backoffLimit,
		TTLSecondsAfterFinished: &ttlSecondsAfterFinished,
		Template: corev1.PodTemplateSpec{
			Spec: *podSpec,
		},
	}
}

func (r *InstorageJobReconciler) buildPodSpec(job *instoragev1alpha1.InstorageJob) *corev1.PodSpec {
	container := r.buildContainer(job)

	podSpec := &corev1.PodSpec{
		RestartPolicy: corev1.RestartPolicyNever,
		Containers:    []corev1.Container{*container},
	}

	if job.Spec.NodeScheduling != nil {
		if job.Spec.NodeScheduling.NodeName != "" {
			podSpec.NodeName = job.Spec.NodeScheduling.NodeName
		}
		if job.Spec.NodeScheduling.NodeSelector != nil {
			podSpec.NodeSelector = job.Spec.NodeScheduling.NodeSelector
		}
	}

	return podSpec
}

func (r *InstorageJobReconciler) buildContainer(job *instoragev1alpha1.InstorageJob) *corev1.Container {
	container := &corev1.Container{
		Name:  "preprocess",
		Image: job.Spec.Image,
	}

	if job.Spec.ImagePullPolicy != "" {
		container.ImagePullPolicy = job.Spec.ImagePullPolicy
	}

	// CSD 설정에 따라 경로 prefix 결정
	dataPath := job.Spec.DataPath
	outputPath := job.Spec.OutputPath

	if job.Spec.CSD != nil && job.Spec.CSD.Enabled {
		if !strings.HasPrefix(dataPath, "/csd/") {
			dataPath = "/home/ngd/storage" + dataPath
		}
		if !strings.HasPrefix(outputPath, "/csd/") {
			outputPath = "/home/ngd/storage" + outputPath
		}
	} else {
		if !strings.HasPrefix(dataPath, "/data/") {
			dataPath = "/mnt" + dataPath
		}
		if !strings.HasPrefix(outputPath, "/data/") {
			outputPath = "/mnt" + outputPath
		}
	}

	env := []corev1.EnvVar{
		{
			Name:  "DATA_PATH",
			Value: dataPath,
		},
		{
			Name:  "OUTPUT_PATH",
			Value: outputPath,
		},
	}

	if job.Spec.Preprocessing != nil {
		if job.Spec.Preprocessing.BatchSize > 0 {
			env = append(env, corev1.EnvVar{
				Name:  "BATCH_SIZE",
				Value: fmt.Sprintf("%d", job.Spec.Preprocessing.BatchSize),
			})
		}
		if job.Spec.Preprocessing.MaxLength > 0 {
			env = append(env, corev1.EnvVar{
				Name:  "MAX_LENGTH",
				Value: fmt.Sprintf("%d", job.Spec.Preprocessing.MaxLength),
			})
		}
		if job.Spec.Preprocessing.NSamples > 0 {
			env = append(env, corev1.EnvVar{
				Name:  "N_SAMPLES",
				Value: fmt.Sprintf("%d", job.Spec.Preprocessing.NSamples),
			})
		}
		if job.Spec.Preprocessing.ParallelWorkers > 0 {
			env = append(env, corev1.EnvVar{
				Name:  "PARALLEL_WORKERS",
				Value: fmt.Sprintf("%d", job.Spec.Preprocessing.ParallelWorkers),
			})
		}
		if job.Spec.Preprocessing.ChunkSize > 0 {
			env = append(env, corev1.EnvVar{
				Name:  "CHUNK_SIZE",
				Value: fmt.Sprintf("%d", job.Spec.Preprocessing.ChunkSize),
			})
		}
	}

	if job.Spec.DataLocations != nil {
		if len(job.Spec.DataLocations.Locations) > 0 {
			env = append(env, corev1.EnvVar{
				Name:  "DATA_LOCATIONS",
				Value: strings.Join(job.Spec.DataLocations.Locations, ","),
			})
		}
		if job.Spec.DataLocations.Strategy != "" {
			env = append(env, corev1.EnvVar{
				Name:  "DATA_STRATEGY",
				Value: job.Spec.DataLocations.Strategy,
			})
		}
	}

	if job.Spec.CSD != nil && job.Spec.CSD.Enabled {
		env = append(env, corev1.EnvVar{
			Name:  "CSD_ENABLED",
			Value: "true",
		})
		if job.Spec.CSD.DevicePath != "" {
			env = append(env, corev1.EnvVar{
				Name:  "CSD_DEVICE_PATH",
				Value: job.Spec.CSD.DevicePath,
			})
		}
	}

	container.Env = env

	if job.Spec.Resources != nil {
		resourceRequirements := corev1.ResourceRequirements{}

		if job.Spec.Resources.Requests != nil {
			resourceRequirements.Requests = make(corev1.ResourceList)
			if job.Spec.Resources.Requests.CPU != "" {
				resourceRequirements.Requests[corev1.ResourceCPU] = resource.MustParse(job.Spec.Resources.Requests.CPU)
			}
			if job.Spec.Resources.Requests.Memory != "" {
				resourceRequirements.Requests[corev1.ResourceMemory] = resource.MustParse(job.Spec.Resources.Requests.Memory)
			}
		}

		if job.Spec.Resources.Limits != nil {
			resourceRequirements.Limits = make(corev1.ResourceList)
			if job.Spec.Resources.Limits.CPU != "" {
				resourceRequirements.Limits[corev1.ResourceCPU] = resource.MustParse(job.Spec.Resources.Limits.CPU)
			}
			if job.Spec.Resources.Limits.Memory != "" {
				resourceRequirements.Limits[corev1.ResourceMemory] = resource.MustParse(job.Spec.Resources.Limits.Memory)
			}
		}

		container.Resources = resourceRequirements
	}

	return container
}

// selectCSDNode - CSD 노드 선택 로직
func (r *InstorageJobReconciler) selectCSDNode(job *instoragev1alpha1.InstorageJob) (string, error) {
	// 1. 먼저 nodeScheduling.nodeName이 지정되어 있는지 확인
	if job.Spec.NodeScheduling != nil && job.Spec.NodeScheduling.NodeName != "" {
		specifiedNode := job.Spec.NodeScheduling.NodeName
		r.Log.Info("Using specified node from nodeScheduling", "node", specifiedNode, "job", job.Name)

		// 지정된 노드가 CSD 노드인지 확인
		node := &corev1.Node{}
		if err := r.Get(context.Background(), types.NamespacedName{Name: specifiedNode}, node); err != nil {
			return "", fmt.Errorf("specified node %s not found: %w", specifiedNode, err)
		}

		// CSD 라벨 확인
		if node.Labels["csd.enabled"] != "true" || node.Labels["csd.status"] != "ready" {
			return "", fmt.Errorf("specified node %s is not a ready CSD node", specifiedNode)
		}

		return specifiedNode, nil
	}

	// 2. nodeScheduling.nodeName이 없으면 CSD 노드 라벨로 필터링
	nodes := &corev1.NodeList{}
	labelSelector := labels.SelectorFromSet(labels.Set{
		"csd.enabled": "true",
		"csd.status":  "ready",
	})

	if err := r.List(context.Background(), nodes, &client.ListOptions{
		LabelSelector: labelSelector,
	}); err != nil {
		return "", fmt.Errorf("failed to list CSD nodes: %w", err)
	}

	if len(nodes.Items) == 0 {
		return "", fmt.Errorf("no CSD nodes available")
	}

	// 현재는 단순히 첫 번째 노드 선택 (추후 스케줄링 로직 개선 가능)
	selectedNode := nodes.Items[0].Name
	r.Log.Info("Selected CSD node", "node", selectedNode, "job", job.Name)

	return selectedNode, nil
}

// Submit job to Instorage Manager via gRPC
func (r *InstorageJobReconciler) submitToInstorageManager(ctx context.Context, nodeName string, job *instoragev1alpha1.InstorageJob) error {
	r.Log.Info("Submitting job to Instorage Manager via gRPC", "node", nodeName, "job", job.Name)

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

// Build gRPC submit job request from InstorageJob spec
func (r *InstorageJobReconciler) buildSubmitJobRequest(job *instoragev1alpha1.InstorageJob, nodeName string) *pb.SubmitJobRequest {
	request := &pb.SubmitJobRequest{
		JobId:           string(job.UID),
		JobName:         job.Name,
		Namespace:       job.Namespace,
		Image:           job.Spec.Image,
		ImagePullPolicy: string(job.Spec.ImagePullPolicy),
		DataPath:        job.Spec.DataPath,
		OutputPath:      job.Spec.OutputPath,
		TargetNode:      nodeName,
		Labels:          job.Labels,
		Annotations:     job.Annotations,
	}

	// Add resources if specified
	if job.Spec.Resources != nil {
		resources := &pb.Resources{}
		if job.Spec.Resources.Requests != nil {
			resources.Requests = &pb.ResourceRequirements{
				Cpu:    job.Spec.Resources.Requests.CPU,
				Memory: job.Spec.Resources.Requests.Memory,
			}
		}
		if job.Spec.Resources.Limits != nil {
			resources.Limits = &pb.ResourceRequirements{
				Cpu:    job.Spec.Resources.Limits.CPU,
				Memory: job.Spec.Resources.Limits.Memory,
			}
		}
		request.Resources = resources
	}

	// Add preprocessing configuration if specified
	if job.Spec.Preprocessing != nil {
		request.Preprocessing = &pb.PreprocessingConfig{
			BatchSize:       int32(job.Spec.Preprocessing.BatchSize),
			MaxLength:       int32(job.Spec.Preprocessing.MaxLength),
			NSamples:        int32(job.Spec.Preprocessing.NSamples),
			ParallelWorkers: int32(job.Spec.Preprocessing.ParallelWorkers),
			ChunkSize:       int32(job.Spec.Preprocessing.ChunkSize),
		}
	}

	// Add data locations if specified
	if job.Spec.DataLocations != nil {
		request.DataLocations = &pb.DataLocations{
			Locations: job.Spec.DataLocations.Locations,
			Strategy:  job.Spec.DataLocations.Strategy,
		}
	}

	// Add CSD configuration if specified
	if job.Spec.CSD != nil {
		request.Csd = &pb.CSDConfig{
			Enabled:    job.Spec.CSD.Enabled,
			DevicePath: job.Spec.CSD.DevicePath,
		}
	}

	// Add node scheduling if specified
	if job.Spec.NodeScheduling != nil {
		request.NodeScheduling = &pb.NodeScheduling{
			NodeName:     job.Spec.NodeScheduling.NodeName,
			NodeSelector: job.Spec.NodeScheduling.NodeSelector,
		}
	}

	// Add job configuration if specified
	if job.Spec.JobConfig != nil {
		request.JobConfig = &pb.JobConfig{
			Parallelism:             int32(job.Spec.JobConfig.Parallelism),
			Completions:             int32(job.Spec.JobConfig.Completions),
			BackoffLimit:            int32(job.Spec.JobConfig.BackoffLimit),
			TtlSecondsAfterFinished: int32(job.Spec.JobConfig.TTLSecondsAfterFinished),
		}
	}

	return request
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

func (r *InstorageJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&instoragev1alpha1.InstorageJob{}).
		Complete(r)
}
