package controller

import (
	"fmt"
	instoragev1alpha1 "instorage-operator/pkg/apis/v1alpha1"
	pb "instorage-operator/pkg/proto"
)

// BatchConfiguration holds calculated batch parameters
type BatchConfiguration struct {
	MaxBatchSize      int
	MaxParallelJobs   int
	ChunkingStrategy  string
	EstimatedDuration int
}

// shouldUseBatchProcessing determines if batch processing should be used
func (r *InstorageJobReconciler) shouldUseBatchProcessing(job *instoragev1alpha1.InstorageJob) bool {
	if job.Spec.Preprocessing == nil {
		return false
	}

	// Use batch processing if:
	// 1. Explicitly requested via strategy (CRD enum: none, auto, manual, size-based, count-based, memory-based)
	// 2. BatchExecution config is provided
	// 3. Data discovery is enabled (to analyze and split data)
	if job.Spec.Preprocessing.BatchStrategy != "" && job.Spec.Preprocessing.BatchStrategy != "none" {
		// All CRD enum values indicate batch processing should be used
		return true
	}
	if job.Spec.Preprocessing.BatchExecution != nil && job.Spec.Preprocessing.BatchExecution.MaxParallelJobs > 1 {
		return true
	}
	if job.Spec.Preprocessing.DataDiscovery != nil && job.Spec.Preprocessing.DataDiscovery.Enabled {
		return true
	}

	return false
}

// createBatchPlan creates a batch execution plan for the job
func (r *InstorageJobReconciler) createBatchPlan(job *instoragev1alpha1.InstorageJob) (*pb.BatchPlan, error) {
	r.Log.Info("Creating batch plan", "job", job.Name, "strategy", job.Spec.Preprocessing.BatchStrategy)

	// Determine batch strategy
	strategy := job.Spec.Preprocessing.BatchStrategy
	if strategy == "" {
		strategy = "auto"
	}

	var batchPlan *pb.BatchPlan

	// Check if user provided complete batch configuration
	if r.hasCompleteUserBatchConfig(job) {
		r.Log.Info("Using complete user batch configuration, skipping data discovery", "job", job.Name)

		// Build configuration directly from user input
		batchConfig := r.buildConfigFromUserInput(job)

		// Generate batches using user-provided settings (no data analysis needed)
		batches, err := r.generateBatchesFromUserConfig(job, batchConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to generate batches from user config: %w", err)
		}

		batchPlan = &pb.BatchPlan{
			TotalBatches:    int32(len(batches)),
			MaxParallelJobs: int32(batchConfig.MaxParallelJobs),
			Batches:         batches,
			Strategy:        strategy,
		}

		r.Log.Info("Batch plan created from user config",
			"job", job.Name,
			"totalBatches", len(batches),
			"maxParallelJobs", batchConfig.MaxParallelJobs,
		)
	} else {

		// User configuration incomplete - perform data discovery
		r.Log.Info("Incomplete user configuration, performing data discovery", "job", job.Name)

		dataAnalysis, err := r.performDataDiscovery(job)
		if err != nil {
			return nil, fmt.Errorf("data discovery failed: %w", err)
		}

		// Calculate optimal batch configuration
		batchConfig := r.calculateBatchConfiguration(job, dataAnalysis)

		// Generate batch infos
		batches, err := r.generateBatches(job, dataAnalysis, batchConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to generate batches: %w", err)
		}

		batchPlan = &pb.BatchPlan{
			TotalBatches:    int32(len(batches)),
			MaxParallelJobs: int32(batchConfig.MaxParallelJobs),
			Batches:         batches,
			Strategy:        strategy,
			DataAnalysis:    dataAnalysis,
		}

		r.Log.Info("Batch plan created with data discovery",
			"job", job.Name,
			"totalBatches", len(batches),
			"maxParallelJobs", batchConfig.MaxParallelJobs,
			"totalFiles", dataAnalysis.TotalFiles,
		)

	}

	return batchPlan, nil
}

// hasCompleteUserBatchConfig checks if user provided all necessary batch configuration
func (r *InstorageJobReconciler) hasCompleteUserBatchConfig(job *instoragev1alpha1.InstorageJob) bool {
	if job.Spec.Preprocessing.BatchExecution == nil {
		return false
	}

	exec := job.Spec.Preprocessing.BatchExecution
	// User must provide both batch size and parallel jobs for complete config
	return exec.MaxBatchSize > 0 && exec.MaxParallelJobs > 0
}

// buildConfigFromUserInput builds batch configuration using only user-provided values
func (r *InstorageJobReconciler) buildConfigFromUserInput(job *instoragev1alpha1.InstorageJob) *BatchConfiguration {
	exec := job.Spec.Preprocessing.BatchExecution

	config := &BatchConfiguration{
		MaxBatchSize:      exec.MaxBatchSize,
		MaxParallelJobs:   exec.MaxParallelJobs,
		ChunkingStrategy:  "round-robin", // Default
		EstimatedDuration: 300,           // Default 5 minutes
	}

	// Apply user-provided strategy if specified
	if job.Spec.Preprocessing.ChunkingStrategy != "" {
		config.ChunkingStrategy = job.Spec.Preprocessing.ChunkingStrategy
	}

	// Apply target duration if specified
	if exec.TargetBatchDuration > 0 {
		config.EstimatedDuration = exec.TargetBatchDuration
	}

	r.Log.Info("Built config from user input",
		"maxBatchSize", config.MaxBatchSize,
		"maxParallelJobs", config.MaxParallelJobs,
		"chunkingStrategy", config.ChunkingStrategy,
	)

	return config
}

// generateBatchesFromUserConfig creates batches using user configuration without data analysis
func (r *InstorageJobReconciler) generateBatchesFromUserConfig(job *instoragev1alpha1.InstorageJob, config *BatchConfiguration) ([]*pb.BatchInfo, error) {
	// Use nSamples if provided by user, otherwise estimate from basic parameters
	totalItems := DefaultTotalItems // Default assumption
	if job.Spec.Preprocessing.NSamples > 0 {
		totalItems = job.Spec.Preprocessing.NSamples
	}

	batchSize := config.MaxBatchSize
	numBatches := (totalItems + batchSize - 1) / batchSize

	batches := make([]*pb.BatchInfo, numBatches)

	for i := 0; i < numBatches; i++ {
		startIdx := i * batchSize
		endIdx := startIdx + batchSize
		if endIdx > totalItems {
			endIdx = totalItems
		}

		itemCount := endIdx - startIdx

		batches[i] = &pb.BatchInfo{
			BatchId:            fmt.Sprintf("%s-batch-%03d", job.Name, i+1),
			FilePaths:          []string{}, // Will be populated by actual execution
			EstimatedSizeBytes: 0,          // Unknown without data analysis
			ItemCount:          int32(itemCount),
			BatchEnv: map[string]string{
				"BATCH_ID":    fmt.Sprintf("batch-%03d", i+1),
				"BATCH_INDEX": fmt.Sprintf("%d", i),
				"BATCH_SIZE":  fmt.Sprintf("%d", itemCount),
				"START_INDEX": fmt.Sprintf("%d", startIdx),
				"END_INDEX":   fmt.Sprintf("%d", endIdx),
			},
		}

		// Add node affinity if using locality-aware strategy
		if config.ChunkingStrategy == "locality-aware" {
			nodeIndex := i % config.MaxParallelJobs
			batches[i].NodeAffinity = fmt.Sprintf("node-%d", nodeIndex)
		}
	}

	r.Log.Info("Generated batches from user config",
		"totalItems", totalItems,
		"batchSize", batchSize,
		"numBatches", numBatches,
	)

	return batches, nil
}

// performDataDiscovery analyzes the data to understand structure and size
func (r *InstorageJobReconciler) performDataDiscovery(job *instoragev1alpha1.InstorageJob) (*pb.DataAnalysisResult, error) {
	r.Log.Info("Performing data discovery", "job", job.Name, "dataPath", job.Spec.DataPath)

	var result *pb.DataAnalysisResult

	// Check if user provided data analysis information
	if job.Spec.Preprocessing.DataDiscovery != nil && job.Spec.Preprocessing.DataDiscovery.UserProvidedData != nil {
		userData := job.Spec.Preprocessing.DataDiscovery.UserProvidedData
		r.Log.Info("Using user-provided data analysis", "job", job.Name)

		result = &pb.DataAnalysisResult{
			TotalFiles:     int32(userData.TotalFiles),
			TotalSizeBytes: int64(userData.TotalSizeBytes),
			AvgFileSize:    int64(userData.AvgFileSize),
			FileTypes:      make(map[string]int32),
			SampledFiles:   int32(userData.TotalFiles), // Assume all files analyzed when user provides data
		}

		// Copy file types if provided
		if userData.FileTypes != nil {
			for ext, count := range userData.FileTypes {
				result.FileTypes[ext] = int32(count)
			}
		}
	} else {
		// Fall back to mock data if no user data provided
		// This is a simplified implementation
		// In a real scenario, you would scan the actual filesystem
		r.Log.Info("Using mock data for discovery", "job", job.Name)
		result = &pb.DataAnalysisResult{
			TotalFiles:     1000,               // Mock data - would scan actual directory
			TotalSizeBytes: 1024 * 1024 * 1024, // 1GB
			AvgFileSize:    1024 * 1024,        // 1MB average
			FileTypes: map[string]int32{
				".txt":  500,
				".json": 300,
				".csv":  200,
			},
			SampledFiles: 100,
		}

		// Apply discovery configuration if provided
		if job.Spec.Preprocessing.DataDiscovery != nil {
			discovery := job.Spec.Preprocessing.DataDiscovery
			if discovery.SampleSize > 0 {
				result.SampledFiles = int32(discovery.SampleSize)
			}
			// TODO: Apply file extension filters, exclude patterns, etc.
		}
	}

	return result, nil
}

// calculateBatchConfiguration determines optimal batch settings
func (r *InstorageJobReconciler) calculateBatchConfiguration(job *instoragev1alpha1.InstorageJob, analysis *pb.DataAnalysisResult) *BatchConfiguration {
	config := &BatchConfiguration{
		MaxBatchSize:      100,           // Default
		MaxParallelJobs:   5,             // Default
		ChunkingStrategy:  "round-robin", // Default
		EstimatedDuration: 300,           // 5 minutes default
	}

	// Apply user configuration if provided
	if job.Spec.Preprocessing.BatchExecution != nil {
		exec := job.Spec.Preprocessing.BatchExecution
		if exec.MaxBatchSize > 0 {
			config.MaxBatchSize = exec.MaxBatchSize
		}
		if exec.MaxParallelJobs > 0 {
			config.MaxParallelJobs = exec.MaxParallelJobs
		}
		if exec.TargetBatchDuration > 0 {
			config.EstimatedDuration = exec.TargetBatchDuration
		}
	}

	if job.Spec.Preprocessing.ChunkingStrategy != "" {
		config.ChunkingStrategy = job.Spec.Preprocessing.ChunkingStrategy
	}

	// Auto-optimize based on data analysis
	if job.Spec.Preprocessing.BatchStrategy == "auto" {
		config = r.optimizeBatchConfiguration(config, analysis)
	}

	return config
}

// optimizeBatchConfiguration automatically optimizes batch settings
func (r *InstorageJobReconciler) optimizeBatchConfiguration(config *BatchConfiguration, analysis *pb.DataAnalysisResult) *BatchConfiguration {
	totalFiles := int(analysis.TotalFiles)
	avgFileSize := analysis.AvgFileSize

	// Adjust batch size based on file count and size
	if totalFiles > 10000 {
		// Many files: smaller batches, more parallelism
		config.MaxBatchSize = DefaultMaxBatchSizeLarge
		config.MaxParallelJobs = DefaultMaxParallelLarge
	} else if totalFiles > 1000 {
		// Medium files: medium batches
		config.MaxBatchSize = DefaultMaxBatchSizeMedium
		config.MaxParallelJobs = DefaultMaxParallelMedium
	} else {
		// Few files: larger batches, less parallelism
		config.MaxBatchSize = DefaultMaxBatchSizeSmall
		config.MaxParallelJobs = 5
	}

	// Adjust based on file size
	if avgFileSize > 100*1024*1024 { // > 100MB files
		// Large files: fewer per batch
		config.MaxBatchSize = config.MaxBatchSize / 2
	} else if avgFileSize < 1024*1024 { // < 1MB files
		// Small files: more per batch
		config.MaxBatchSize = config.MaxBatchSize * 2
	}

	// Ensure reasonable limits
	if config.MaxBatchSize > DefaultMaxBatchSizeHuge {
		config.MaxBatchSize = DefaultMaxBatchSizeMedium
	}
	if config.MaxBatchSize < DefaultMaxBatchSizeTiny {
		config.MaxBatchSize = DefaultMaxBatchSizeTiny
	}
	if config.MaxParallelJobs > DefaultMaxParallelHuge {
		config.MaxParallelJobs = DefaultMaxParallelHuge
	}
	if config.MaxParallelJobs < DefaultMaxParallelTiny {
		config.MaxParallelJobs = DefaultMaxParallelTiny
	}

	return config
}

// generateBatches creates individual batch information
func (r *InstorageJobReconciler) generateBatches(job *instoragev1alpha1.InstorageJob, analysis *pb.DataAnalysisResult, config *BatchConfiguration) ([]*pb.BatchInfo, error) {
	totalFiles := int(analysis.TotalFiles)
	batchSize := config.MaxBatchSize

	// Calculate number of batches needed
	numBatches := (totalFiles + batchSize - 1) / batchSize

	batches := make([]*pb.BatchInfo, numBatches)

	for i := 0; i < numBatches; i++ {
		startIdx := i * batchSize
		endIdx := startIdx + batchSize
		if endIdx > totalFiles {
			endIdx = totalFiles
		}

		// Generate mock file paths (in real implementation, would use actual file discovery)
		filePaths := make([]string, endIdx-startIdx)
		for j := startIdx; j < endIdx; j++ {
			filePaths[j-startIdx] = fmt.Sprintf("%s/file_%06d.txt", job.Spec.DataPath, j)
		}

		// Estimate batch size
		estimatedSize := int64(len(filePaths)) * analysis.AvgFileSize

		batches[i] = &pb.BatchInfo{
			BatchId:            fmt.Sprintf("%s-batch-%03d", job.Name, i+1),
			FilePaths:          filePaths,
			EstimatedSizeBytes: estimatedSize,
			ItemCount:          int32(len(filePaths)),
			BatchEnv: map[string]string{
				"BATCH_ID":    fmt.Sprintf("batch-%03d", i+1),
				"BATCH_INDEX": fmt.Sprintf("%d", i),
				"BATCH_SIZE":  fmt.Sprintf("%d", len(filePaths)),
			},
		}

		// Add node affinity if using locality-aware strategy
		if config.ChunkingStrategy == "locality-aware" {
			// Simple round-robin node assignment
			nodeIndex := i % config.MaxParallelJobs
			batches[i].NodeAffinity = fmt.Sprintf("node-%d", nodeIndex)
		}
	}

	return batches, nil
}
