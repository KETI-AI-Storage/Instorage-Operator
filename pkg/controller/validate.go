package controller

import (
	"fmt"
	instoragev1alpha1 "instorage-operator/pkg/apis/v1alpha1"
	"path/filepath"
	"regexp"

	"k8s.io/apimachinery/pkg/api/resource"
)

func (r *InstorageJobReconciler) validateJobSpec(job *instoragev1alpha1.InstorageJob) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}

	// Validate basic job metadata
	if job.Name == "" {
		return fmt.Errorf("job name is required")
	}
	// Note: InstorageJob is cluster-scoped, so no namespace validation needed

	// Validate required job spec fields
	if job.Spec.Type == "" {
		return fmt.Errorf("job type is required")
	}
	if job.Spec.Image == "" {
		return fmt.Errorf("container image is required")
	}
	if job.Spec.DataPath == "" {
		return fmt.Errorf("dataPath is required")
	}
	if job.Spec.OutputPath == "" {
		return fmt.Errorf("outputPath is required")
	}

	// Validate job type
	validTypes := map[string]bool{
		"preprocess": true,
		"inference":  true,
		"training":   true,
	}
	if !validTypes[job.Spec.Type] {
		return fmt.Errorf("invalid job type '%s', must be one of: preprocess, inference, training", job.Spec.Type)
	}

	// Validate image format (basic check)
	if !r.isValidImageName(job.Spec.Image) {
		return fmt.Errorf("invalid container image format: %s", job.Spec.Image)
	}

	// Validate paths are absolute
	if !filepath.IsAbs(job.Spec.DataPath) {
		return fmt.Errorf("dataPath must be an absolute path: %s", job.Spec.DataPath)
	}
	if !filepath.IsAbs(job.Spec.OutputPath) {
		return fmt.Errorf("outputPath must be an absolute path: %s", job.Spec.OutputPath)
	}

	// Validate resources if specified
	if job.Spec.Resources != nil {
		if err := r.validateResourceConfig(job.Spec.Resources); err != nil {
			return fmt.Errorf("invalid resource specification: %w", err)
		}
	}

	// Validate preprocessing configuration if specified
	if job.Spec.Preprocessing != nil {
		if err := r.validatePreprocessingConfig(job.Spec.Preprocessing); err != nil {
			return fmt.Errorf("invalid preprocessing configuration: %w", err)
		}
	}

	// Validate data locations if specified
	if job.Spec.DataLocations != nil {
		if err := r.validateDataLocations(job.Spec.DataLocations); err != nil {
			return fmt.Errorf("invalid data locations configuration: %w", err)
		}
	}

	// Validate CSD configuration if specified
	if job.Spec.CSD != nil {
		if err := r.validateCSDConfig(job.Spec.CSD); err != nil {
			return fmt.Errorf("invalid CSD configuration: %w", err)
		}
	}

	// Validate node scheduling if specified
	if job.Spec.NodeScheduling != nil {
		if err := r.validateNodeScheduling(job.Spec.NodeScheduling); err != nil {
			return fmt.Errorf("invalid node scheduling configuration: %w", err)
		}
	}

	// Validate job configuration if specified
	if job.Spec.JobConfig != nil {
		if err := r.validateJobConfig(job.Spec.JobConfig); err != nil {
			return fmt.Errorf("invalid job configuration: %w", err)
		}
	}

	return nil
}

// isValidImageName performs basic validation on container image name
func (r *InstorageJobReconciler) isValidImageName(image string) bool {
	if len(image) == 0 || len(image) > MaxImageNameLength {
		return false
	}

	// Basic regex for container image validation
	// Allows registry/namespace/name:tag format
	validImagePattern := `^[a-zA-Z0-9]([a-zA-Z0-9._-]*[a-zA-Z0-9])?(/[a-zA-Z0-9]([a-zA-Z0-9._-]*[a-zA-Z0-9])?)*(:([a-zA-Z0-9._-]+))?$`
	matched, _ := regexp.MatchString(validImagePattern, image)
	return matched
}

// validateResourceConfig validates resource specifications
func (r *InstorageJobReconciler) validateResourceConfig(resources *instoragev1alpha1.ResourceConfig) error {
	if resources.Requests != nil {
		if err := r.validateResourceList(resources.Requests, "requests"); err != nil {
			return err
		}
	}

	if resources.Limits != nil {
		if err := r.validateResourceList(resources.Limits, "limits"); err != nil {
			return err
		}
	}

	// Validate that limits >= requests if both are specified
	if resources.Requests != nil && resources.Limits != nil {
		if err := r.validateResourceConstraints(resources.Requests, resources.Limits); err != nil {
			return err
		}
	}

	return nil
}

// validateResourceRequirements validates individual resource requirements
func (r *InstorageJobReconciler) validateResourceList(req *instoragev1alpha1.ResourceList, reqType string) error {
	if req.CPU != "" {
		if _, err := resource.ParseQuantity(req.CPU); err != nil {
			return fmt.Errorf("invalid CPU %s: %s (%w)", reqType, req.CPU, err)
		}
	}

	if req.Memory != "" {
		if _, err := resource.ParseQuantity(req.Memory); err != nil {
			return fmt.Errorf("invalid Memory %s: %s (%w)", reqType, req.Memory, err)
		}
	}

	return nil
}

// validateResourceConstraints ensures limits are greater than or equal to requests
func (r *InstorageJobReconciler) validateResourceConstraints(requests, limits *instoragev1alpha1.ResourceList) error {
	if requests.CPU != "" && limits.CPU != "" {
		reqCPU, _ := resource.ParseQuantity(requests.CPU)
		limitCPU, _ := resource.ParseQuantity(limits.CPU)
		if limitCPU.Cmp(reqCPU) < 0 {
			return fmt.Errorf("CPU limits (%s) must be greater than or equal to requests (%s)", limits.CPU, requests.CPU)
		}
	}

	if requests.Memory != "" && limits.Memory != "" {
		reqMemory, _ := resource.ParseQuantity(requests.Memory)
		limitMemory, _ := resource.ParseQuantity(limits.Memory)
		if limitMemory.Cmp(reqMemory) < 0 {
			return fmt.Errorf("Memory limits (%s) must be greater than or equal to requests (%s)", limits.Memory, requests.Memory)
		}
	}

	return nil
}

// validatePreprocessingConfig validates preprocessing configuration
func (r *InstorageJobReconciler) validatePreprocessingConfig(config *instoragev1alpha1.PreprocessingConfig) error {
	if config.BatchSize <= 0 {
		return fmt.Errorf("batchSize must be greater than 0, got: %d", config.BatchSize)
	}
	if config.BatchSize > MaxBatchSizeLimit {
		return fmt.Errorf("batchSize too large (max: %d), got: %d", MaxBatchSizeLimit, config.BatchSize)
	}

	if config.MaxLength <= 0 {
		return fmt.Errorf("maxLength must be greater than 0, got: %d", config.MaxLength)
	}

	if config.NSamples < 0 {
		return fmt.Errorf("nSamples cannot be negative, got: %d", config.NSamples)
	}

	if config.ParallelWorkers <= 0 {
		return fmt.Errorf("parallelWorkers must be greater than 0, got: %d", config.ParallelWorkers)
	}
	if config.ParallelWorkers > 100 {
		return fmt.Errorf("parallelWorkers too large (max: 100), got: %d", config.ParallelWorkers)
	}

	if config.ChunkSize <= 0 {
		return fmt.Errorf("chunkSize must be greater than 0, got: %d", config.ChunkSize)
	}

	return nil
}

// validateDataLocations validates data locations configuration
func (r *InstorageJobReconciler) validateDataLocations(config *instoragev1alpha1.DataLocations) error {
	if len(config.Locations) == 0 {
		return fmt.Errorf("at least one data location must be specified")
	}

	validStrategies := map[string]bool{
		"co-locate":  true,
		"fan-out":    true,
		"replicated": true,
	}

	if config.Strategy != "" && !validStrategies[config.Strategy] {
		return fmt.Errorf("invalid strategy '%s', must be one of: co-locate, fan-out, replicated", config.Strategy)
	}

	// Validate each location ID (should match pattern: csd[0-9]+)
	for i, location := range config.Locations {
		if location == "" {
			return fmt.Errorf("data location at index %d cannot be empty", i)
		}
		// Validate CSD ID format
		matched, _ := regexp.MatchString("^csd[0-9]+$", location)
		if !matched {
			return fmt.Errorf("data location at index %d must be a valid CSD ID (e.g., csd1, csd2): %s", i, location)
		}
	}

	return nil
}

// validateCSDConfig validates CSD configuration
func (r *InstorageJobReconciler) validateCSDConfig(config *instoragev1alpha1.CSDConfig) error {
	if config == nil {
		return nil
	}

	return nil
}

// validateNodeScheduling validates node scheduling configuration
func (r *InstorageJobReconciler) validateNodeScheduling(config *instoragev1alpha1.NodeScheduling) error {
	if config.NodeName != "" {
		// Basic node name validation
		if len(config.NodeName) > 63 {
			return fmt.Errorf("nodeName too long (max: 63 chars): %s", config.NodeName)
		}
		// Kubernetes node name should be a valid DNS subdomain
		validNodeName := `^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`
		matched, _ := regexp.MatchString(validNodeName, config.NodeName)
		if !matched {
			return fmt.Errorf("invalid nodeName format (must be valid DNS subdomain): %s", config.NodeName)
		}
	}

	// Validate node selector labels
	for key, value := range config.NodeSelector {
		if key == "" {
			return fmt.Errorf("nodeSelector key cannot be empty")
		}
		if len(key) > 63 {
			return fmt.Errorf("nodeSelector key too long (max: 63 chars): %s", key)
		}
		if len(value) > 63 {
			return fmt.Errorf("nodeSelector value too long (max: 63 chars): %s", value)
		}
	}

	return nil
}

// validateJobConfig validates job configuration
func (r *InstorageJobReconciler) validateJobConfig(config *instoragev1alpha1.JobConfig) error {
	if config.Parallelism < 0 {
		return fmt.Errorf("parallelism cannot be negative, got: %d", config.Parallelism)
	}
	if config.Parallelism > 1000 {
		return fmt.Errorf("parallelism too large (max: 1000), got: %d", config.Parallelism)
	}

	if config.Completions < 0 {
		return fmt.Errorf("completions cannot be negative, got: %d", config.Completions)
	}

	if config.BackoffLimit < 0 {
		return fmt.Errorf("backoffLimit cannot be negative, got: %d", config.BackoffLimit)
	}
	if config.BackoffLimit > 10 {
		return fmt.Errorf("backoffLimit too large (max: 10), got: %d", config.BackoffLimit)
	}

	if config.TTLSecondsAfterFinished < 0 {
		return fmt.Errorf("ttlSecondsAfterFinished cannot be negative, got: %d", config.TTLSecondsAfterFinished)
	}

	return nil
}
