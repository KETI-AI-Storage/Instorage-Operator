package controller

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	instoragev1alpha1 "instorage-operator/pkg/apis/v1alpha1"
	pb "instorage-operator/pkg/proto"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TODO
// 1. performDataDiscovery : 데이터 분석 과정 구현 필요, 현재 Mock Data
// 2. optimizeBatchConfiguration + optimizeBatchConfiguration : 최적 배치 전략 개선 필요

// Path prefixes based on CSD enabled status
const (
	CSDEnabledPathPrefix  = "/home/ngd/storage"
	CSDDisabledPathPrefix = "/mnt"
)

// buildStoragePath builds the full storage path based on CSD configuration
func (r *InstorageJobReconciler) buildStoragePath(originalPath string, csdEnabled bool) string {
	// If path is already absolute and contains the expected prefix, return as-is
	if filepath.IsAbs(originalPath) {
		if csdEnabled && strings.HasPrefix(originalPath, CSDEnabledPathPrefix) {
			return originalPath
		}
		if !csdEnabled && strings.HasPrefix(originalPath, CSDDisabledPathPrefix) {
			return originalPath
		}
	}

	// Add appropriate prefix
	prefix := CSDDisabledPathPrefix
	if csdEnabled {
		prefix = CSDEnabledPathPrefix
	}

	// Clean the original path to remove leading slash if present
	cleanPath := strings.TrimPrefix(originalPath, "/")

	return filepath.Join(prefix, cleanPath)
}

func (r *InstorageJobReconciler) createKubernetesJob(ctx context.Context, job *instoragev1alpha1.InstorageJob) (*batchv1.Job, error) {
	jobSpec := r.buildJobSpec(job)

	// Use default namespace for cluster-scoped InstorageJob
	namespace := job.Namespace
	if namespace == "" {
		namespace = "default"
	}

	k8sJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      job.Name,
			Namespace: namespace,
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
	parallelism := int32(DefaultParallelism)
	completions := int32(DefaultCompletions)
	backoffLimit := int32(DefaultBackoffLimit)
	ttlSecondsAfterFinished := int32(DefaultTTLSecondsAfterFinished)

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

	// Determine if CSD is enabled and build appropriate paths
	csdEnabled := job.Spec.CSD != nil && job.Spec.CSD.Enabled
	dataPath := r.buildStoragePath(job.Spec.DataPath, csdEnabled)
	outputPath := r.buildStoragePath(job.Spec.OutputPath, csdEnabled)

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

// createBatchKubernetesJob creates a Kubernetes Job for a specific batch
func (r *InstorageJobReconciler) createBatchKubernetesJob(ctx context.Context, parentJob *instoragev1alpha1.InstorageJob, batch *pb.BatchInfo, batchIndex int) (*batchv1.Job, error) {
	batchJobName := fmt.Sprintf("%s-batch-%d", parentJob.Name, batchIndex)

	jobSpec := r.buildBatchJobSpec(parentJob, batch)

	k8sJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      batchJobName,
			Namespace: parentJob.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":       "instorage-batch-job",
				"app.kubernetes.io/instance":   batchJobName,
				"app.kubernetes.io/managed-by": "instorage-operator",
				"instorage.job.parent":         parentJob.Name,
				"instorage.job.type":           "batch",
				"instorage.batch.id":           batch.BatchId,
			},
			Annotations: map[string]string{
				"instorage.io/batch-item-count": fmt.Sprintf("%d", batch.ItemCount),
				"instorage.io/batch-size":       fmt.Sprintf("%d", batch.EstimatedSizeBytes),
			},
		},
		Spec: *jobSpec,
	}

	return k8sJob, nil
}

// buildBatchJobSpec builds job spec for a specific batch
func (r *InstorageJobReconciler) buildBatchJobSpec(parentJob *instoragev1alpha1.InstorageJob, batch *pb.BatchInfo) *batchv1.JobSpec {
	// 배치 작업은 일반적으로 단일 완료를 목표로 함
	parallelism := int32(DefaultParallelism)
	completions := int32(DefaultCompletions)
	backoffLimit := int32(DefaultBackoffLimit)
	ttlSecondsAfterFinished := int32(DefaultTTLSecondsAfterFinished)

	// 부모 Job의 설정 적용
	if parentJob.Spec.JobConfig != nil {
		if parentJob.Spec.JobConfig.BackoffLimit > 0 {
			backoffLimit = int32(parentJob.Spec.JobConfig.BackoffLimit)
		}
		if parentJob.Spec.JobConfig.TTLSecondsAfterFinished > 0 {
			ttlSecondsAfterFinished = int32(parentJob.Spec.JobConfig.TTLSecondsAfterFinished)
		}
	}

	podSpec := r.buildBatchPodSpec(parentJob, batch)

	return &batchv1.JobSpec{
		Parallelism:             &parallelism,
		Completions:             &completions,
		BackoffLimit:            &backoffLimit,
		TTLSecondsAfterFinished: &ttlSecondsAfterFinished,
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					"instorage.io/batch-id": batch.BatchId,
				},
			},
			Spec: *podSpec,
		},
	}
}

// buildBatchPodSpec builds pod spec for a specific batch
func (r *InstorageJobReconciler) buildBatchPodSpec(parentJob *instoragev1alpha1.InstorageJob, batch *pb.BatchInfo) *corev1.PodSpec {
	container := r.buildBatchContainer(parentJob, batch)

	podSpec := &corev1.PodSpec{
		RestartPolicy: corev1.RestartPolicyNever,
		Containers:    []corev1.Container{*container},
	}

	// Node scheduling 적용 (배치별 node affinity 고려)
	if parentJob.Spec.NodeScheduling != nil {
		if parentJob.Spec.NodeScheduling.NodeName != "" {
			podSpec.NodeName = parentJob.Spec.NodeScheduling.NodeName
		}
		if parentJob.Spec.NodeScheduling.NodeSelector != nil {
			podSpec.NodeSelector = parentJob.Spec.NodeScheduling.NodeSelector
		}
	}

	// 배치별 node affinity가 지정된 경우
	if batch.NodeAffinity != "" {
		if podSpec.NodeSelector == nil {
			podSpec.NodeSelector = make(map[string]string)
		}
		podSpec.NodeSelector["kubernetes.io/hostname"] = batch.NodeAffinity
	}

	return podSpec
}

// buildBatchContainer builds container spec for a specific batch
func (r *InstorageJobReconciler) buildBatchContainer(parentJob *instoragev1alpha1.InstorageJob, batch *pb.BatchInfo) *corev1.Container {
	container := &corev1.Container{
		Name:  "preprocess-batch",
		Image: parentJob.Spec.Image,
	}

	if parentJob.Spec.ImagePullPolicy != "" {
		container.ImagePullPolicy = parentJob.Spec.ImagePullPolicy
	}

	dataPath := "/mnt" + parentJob.Spec.DataPath
	outputPath := "/mnt" + parentJob.Spec.OutputPath + "/batch-" + batch.BatchId

	// 기본 환경변수
	env := []corev1.EnvVar{
		{
			Name:  "DATA_PATH",
			Value: dataPath,
		},
		{
			Name:  "OUTPUT_PATH",
			Value: outputPath,
		},
		{
			Name:  "BATCH_ID",
			Value: batch.BatchId,
		},
		{
			Name:  "BATCH_ITEM_COUNT",
			Value: fmt.Sprintf("%d", batch.ItemCount),
		},
	}

	// 배치별 파일 목록 전달
	if len(batch.FilePaths) > 0 {
		env = append(env, corev1.EnvVar{
			Name:  "BATCH_FILE_PATHS",
			Value: strings.Join(batch.FilePaths, ","),
		})
	}

	// 배치별 환경변수 추가
	for key, value := range batch.BatchEnv {
		env = append(env, corev1.EnvVar{
			Name:  key,
			Value: value,
		})
	}

	// 전처리 설정 추가
	if parentJob.Spec.Preprocessing != nil {
		if parentJob.Spec.Preprocessing.BatchSize > 0 {
			env = append(env, corev1.EnvVar{
				Name:  "BATCH_SIZE",
				Value: fmt.Sprintf("%d", parentJob.Spec.Preprocessing.BatchSize),
			})
		}
		if parentJob.Spec.Preprocessing.MaxLength > 0 {
			env = append(env, corev1.EnvVar{
				Name:  "MAX_LENGTH",
				Value: fmt.Sprintf("%d", parentJob.Spec.Preprocessing.MaxLength),
			})
		}
		if parentJob.Spec.Preprocessing.NSamples > 0 {
			env = append(env, corev1.EnvVar{
				Name:  "N_SAMPLES",
				Value: fmt.Sprintf("%d", parentJob.Spec.Preprocessing.NSamples),
			})
		}
		if parentJob.Spec.Preprocessing.ParallelWorkers > 0 {
			env = append(env, corev1.EnvVar{
				Name:  "PARALLEL_WORKERS",
				Value: fmt.Sprintf("%d", parentJob.Spec.Preprocessing.ParallelWorkers),
			})
		}
		if parentJob.Spec.Preprocessing.ChunkSize > 0 {
			env = append(env, corev1.EnvVar{
				Name:  "CHUNK_SIZE",
				Value: fmt.Sprintf("%d", parentJob.Spec.Preprocessing.ChunkSize),
			})
		}
	}

	// 데이터 위치 정보 추가
	if parentJob.Spec.DataLocations != nil {
		if len(parentJob.Spec.DataLocations.Locations) > 0 {
			env = append(env, corev1.EnvVar{
				Name:  "DATA_LOCATIONS",
				Value: strings.Join(parentJob.Spec.DataLocations.Locations, ","),
			})
		}
		if parentJob.Spec.DataLocations.Strategy != "" {
			env = append(env, corev1.EnvVar{
				Name:  "DATA_STRATEGY",
				Value: parentJob.Spec.DataLocations.Strategy,
			})
		}
	}

	// CSD 설정 추가
	if parentJob.Spec.CSD != nil && parentJob.Spec.CSD.Enabled {
		env = append(env, corev1.EnvVar{
			Name:  "CSD_ENABLED",
			Value: "true",
		})
		if parentJob.Spec.CSD.DevicePath != "" {
			env = append(env, corev1.EnvVar{
				Name:  "CSD_DEVICE_PATH",
				Value: parentJob.Spec.CSD.DevicePath,
			})
		}
	}

	container.Env = env

	// 리소스 설정 적용
	if parentJob.Spec.Resources != nil {
		resourceRequirements := corev1.ResourceRequirements{}

		if parentJob.Spec.Resources.Requests != nil {
			resourceRequirements.Requests = make(corev1.ResourceList)
			if parentJob.Spec.Resources.Requests.CPU != "" {
				resourceRequirements.Requests[corev1.ResourceCPU] = resource.MustParse(parentJob.Spec.Resources.Requests.CPU)
			}
			if parentJob.Spec.Resources.Requests.Memory != "" {
				resourceRequirements.Requests[corev1.ResourceMemory] = resource.MustParse(parentJob.Spec.Resources.Requests.Memory)
			}
		}

		if parentJob.Spec.Resources.Limits != nil {
			resourceRequirements.Limits = make(corev1.ResourceList)
			if parentJob.Spec.Resources.Limits.CPU != "" {
				resourceRequirements.Limits[corev1.ResourceCPU] = resource.MustParse(parentJob.Spec.Resources.Limits.CPU)
			}
			if parentJob.Spec.Resources.Limits.Memory != "" {
				resourceRequirements.Limits[corev1.ResourceMemory] = resource.MustParse(parentJob.Spec.Resources.Limits.Memory)
			}
		}

		container.Resources = resourceRequirements
	}

	return container
}

// Build gRPC submit job request from InstorageJob spec
func (r *InstorageJobReconciler) buildSubmitJobRequest(job *instoragev1alpha1.InstorageJob, nodeName string) *pb.SubmitJobRequest {
	// Determine if CSD is enabled
	csdEnabled := job.Spec.CSD != nil && job.Spec.CSD.Enabled

	// Build storage paths with appropriate prefixes
	dataPath := r.buildStoragePath(job.Spec.DataPath, csdEnabled)
	outputPath := r.buildStoragePath(job.Spec.OutputPath, csdEnabled)

	namespace := job.Namespace
	if namespace == "" {
		namespace = "default"
	}

	request := &pb.SubmitJobRequest{
		JobId:           string(job.UID),
		JobName:         job.Name,
		Namespace:       namespace,
		Image:           job.Spec.Image,
		ImagePullPolicy: string(job.Spec.ImagePullPolicy),
		DataPath:        dataPath,
		OutputPath:      outputPath,
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
		preprocessing := &pb.PreprocessingConfig{
			BatchSize:        int32(job.Spec.Preprocessing.BatchSize),
			MaxLength:        int32(job.Spec.Preprocessing.MaxLength),
			NSamples:         int32(job.Spec.Preprocessing.NSamples),
			ParallelWorkers:  int32(job.Spec.Preprocessing.ParallelWorkers),
			ChunkSize:        int32(job.Spec.Preprocessing.ChunkSize),
			BatchStrategy:    job.Spec.Preprocessing.BatchStrategy,
			ChunkingStrategy: job.Spec.Preprocessing.ChunkingStrategy,
		}

		// Add batch execution config
		if job.Spec.Preprocessing.BatchExecution != nil {
			preprocessing.BatchExecution = &pb.BatchExecution{
				MaxBatchSize:        int32(job.Spec.Preprocessing.BatchExecution.MaxBatchSize),
				MaxParallelJobs:     int32(job.Spec.Preprocessing.BatchExecution.MaxParallelJobs),
				MinParallelJobs:     int32(job.Spec.Preprocessing.BatchExecution.MinParallelJobs),
				TargetBatchDuration: int32(job.Spec.Preprocessing.BatchExecution.TargetBatchDuration),
				BatchTimeout:        int32(job.Spec.Preprocessing.BatchExecution.BatchTimeout),
			}
		}

		// Add data discovery config
		if job.Spec.Preprocessing.DataDiscovery != nil {
			preprocessing.DataDiscovery = &pb.DataDiscovery{
				Enabled:         job.Spec.Preprocessing.DataDiscovery.Enabled,
				SampleSize:      int32(job.Spec.Preprocessing.DataDiscovery.SampleSize),
				SampleRatio:     job.Spec.Preprocessing.DataDiscovery.SampleRatio,
				MaxScanDepth:    int32(job.Spec.Preprocessing.DataDiscovery.MaxScanDepth),
				FileExtensions:  job.Spec.Preprocessing.DataDiscovery.FileExtensions,
				ExcludePatterns: job.Spec.Preprocessing.DataDiscovery.ExcludePatterns,
			}
		}

		// Add dynamic scaling config
		if job.Spec.Preprocessing.DynamicScaling != nil {
			preprocessing.DynamicScaling = &pb.DynamicScaling{
				Enabled:            job.Spec.Preprocessing.DynamicScaling.Enabled,
				ScaleUpThreshold:   job.Spec.Preprocessing.DynamicScaling.ScaleUpThreshold,
				ScaleDownThreshold: job.Spec.Preprocessing.DynamicScaling.ScaleDownThreshold,
				CooldownPeriod:     int32(job.Spec.Preprocessing.DynamicScaling.CooldownPeriod),
			}
		}

		// Add optimization config
		if job.Spec.Preprocessing.Optimization != nil {
			preprocessing.Optimization = &pb.Optimization{
				EnableCaching:     job.Spec.Preprocessing.Optimization.EnableCaching,
				CacheSize:         job.Spec.Preprocessing.Optimization.CacheSize,
				EnableCompression: job.Spec.Preprocessing.Optimization.EnableCompression,
				IoOptimization:    job.Spec.Preprocessing.Optimization.IoOptimization,
			}
		}

		request.Preprocessing = preprocessing
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
			Enabled: job.Spec.CSD.Enabled,
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

	// Generate batch plan if preprocessing is enabled
	if job.Spec.Preprocessing != nil && r.shouldUseBatchProcessing(job) {
		batchPlan, err := r.createBatchPlan(job)
		if err != nil {
			r.Log.Error(err, "Failed to create batch plan", "job", job.Name)
			// Continue without batch plan for now
		} else {
			request.BatchPlan = batchPlan
		}
	}

	return request
}
