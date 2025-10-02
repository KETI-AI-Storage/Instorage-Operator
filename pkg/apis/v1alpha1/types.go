package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	// SchemeGroupVersion is group version used to register these objects
	SchemeGroupVersion = schema.GroupVersion{Group: "batch.csd.io", Version: "v1alpha1"}

	// SchemeBuilder is used to add go types to the GroupVersionKind scheme
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)

	// AddToScheme adds the types in this group-version to the given scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// InstorageJob is the Schema for the instoragejobs API
type InstorageJob struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              InstorageJobSpec   `json:"spec,omitempty"`
	Status            InstorageJobStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// InstorageJobList contains a list of InstorageJob
type InstorageJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []InstorageJob `json:"items"`
}

// InstorageJobSpec defines the desired state of InstorageJob
type InstorageJobSpec struct {
	Type            string               `json:"type"`
	DataPath        string               `json:"dataPath"`
	OutputPath      string               `json:"outputPath"`
	Image           string               `json:"image"`
	ImagePullPolicy corev1.PullPolicy    `json:"imagePullPolicy,omitempty"`
	CSD             *CSDConfig           `json:"csd,omitempty"`
	DataLocations   *DataLocations       `json:"dataLocations,omitempty"`
	Preprocessing   *PreprocessingConfig `json:"preprocessing,omitempty"`
	Resources       *ResourceConfig      `json:"resources,omitempty"`
	NodeScheduling  *NodeScheduling      `json:"nodeScheduling,omitempty"`
	JobConfig       *JobConfig           `json:"jobConfig,omitempty"`
	Monitoring      *MonitoringConfig    `json:"monitoring,omitempty"`
	RetryPolicy     *RetryPolicy         `json:"retryPolicy,omitempty"`
}

// CSDConfig defines CSD-related configuration
type CSDConfig struct {
	Enabled         bool             `json:"enabled,omitempty"`
	DevicePath      string           `json:"devicePath,omitempty"`
	OffloadingRatio *OffloadingRatio `json:"offloadingRatio,omitempty"`
}

// OffloadingRatio defines offloading ratio configuration
type OffloadingRatio struct {
	Request float64 `json:"request,omitempty"`
	Limit   float64 `json:"limit,omitempty"`
}

// DataLocations defines distributed data location hints
type DataLocations struct {
	Strategy  string           `json:"strategy,omitempty"`
	Locations []string         `json:"locations,omitempty"`
	Weights   []LocationWeight `json:"weights,omitempty"`
}

// LocationWeight defines location with weight
type LocationWeight struct {
	Location string `json:"location"`
	Weight   int    `json:"weight"`
}

// PreprocessingConfig defines preprocessing parameters
type PreprocessingConfig struct {
	BatchSize        int             `json:"batchSize,omitempty"`
	MaxLength        int             `json:"maxLength,omitempty"`
	NSamples         int             `json:"nSamples,omitempty"`
	ParallelWorkers  int             `json:"parallelWorkers,omitempty"`
	ChunkSize        int             `json:"chunkSize,omitempty"`
	BatchStrategy    string          `json:"batchStrategy,omitempty"`
	BatchExecution   *BatchExecution `json:"batchExecution,omitempty"`
	ChunkingStrategy string          `json:"chunkingStrategy,omitempty"`
	DataDiscovery    *DataDiscovery  `json:"dataDiscovery,omitempty"`
	DynamicScaling   *DynamicScaling `json:"dynamicScaling,omitempty"`
	Optimization     *Optimization   `json:"optimization,omitempty"`
}

// BatchExecution defines batch execution parameters
type BatchExecution struct {
	MaxBatchSize        int `json:"maxBatchSize,omitempty"`
	MaxParallelJobs     int `json:"maxParallelJobs,omitempty"`
	MinParallelJobs     int `json:"minParallelJobs,omitempty"`
	TargetBatchDuration int `json:"targetBatchDuration,omitempty"`
	BatchTimeout        int `json:"batchTimeout,omitempty"`
}

// DataDiscovery defines data discovery parameters
type DataDiscovery struct {
	Enabled          bool              `json:"enabled,omitempty"`
	SampleSize       int               `json:"sampleSize,omitempty"`
	SampleRatio      float64           `json:"sampleRatio,omitempty"`
	MaxScanDepth     int               `json:"maxScanDepth,omitempty"`
	FileExtensions   []string          `json:"fileExtensions,omitempty"`
	ExcludePatterns  []string          `json:"excludePatterns,omitempty"`
	UserProvidedData *UserProvidedData `json:"userProvidedData,omitempty"`
}

// UserProvidedData defines user-provided data analysis information
type UserProvidedData struct {
	TotalFiles     int            `json:"totalFiles"`
	TotalSizeBytes int64          `json:"totalSizeBytes"`
	AvgFileSize    int64          `json:"avgFileSize"`
	FileTypes      map[string]int `json:"fileTypes,omitempty"`
}

// DynamicScaling defines dynamic scaling parameters
type DynamicScaling struct {
	Enabled            bool    `json:"enabled,omitempty"`
	ScaleUpThreshold   float64 `json:"scaleUpThreshold,omitempty"`
	ScaleDownThreshold float64 `json:"scaleDownThreshold,omitempty"`
	CooldownPeriod     int     `json:"cooldownPeriod,omitempty"`
}

// Optimization defines performance optimization parameters
type Optimization struct {
	EnableCaching     bool   `json:"enableCaching,omitempty"`
	CacheSize         string `json:"cacheSize,omitempty"`
	EnableCompression bool   `json:"enableCompression,omitempty"`
	IoOptimization    string `json:"ioOptimization,omitempty"`
}

// ResourceConfig defines resource requirements
type ResourceConfig struct {
	Requests *ResourceList `json:"requests,omitempty"`
	Limits   *ResourceList `json:"limits,omitempty"`
}

// ResourceList defines resource list
type ResourceList struct {
	CPU    string `json:"cpu,omitempty"`
	Memory string `json:"memory,omitempty"`
	GPU    string `json:"gpu,omitempty"`
}

// NodeScheduling defines node scheduling configuration
type NodeScheduling struct {
	NodeName     string            `json:"nodeName,omitempty"`
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	Affinity     interface{}       `json:"affinity,omitempty"`
}

// JobConfig defines job behavior configuration
type JobConfig struct {
	Completions             int `json:"completions,omitempty"`
	Parallelism             int `json:"parallelism,omitempty"`
	BackoffLimit            int `json:"backoffLimit,omitempty"`
	TTLSecondsAfterFinished int `json:"ttlSecondsAfterFinished,omitempty"`
	ActiveDeadlineSeconds   int `json:"activeDeadlineSeconds,omitempty"`
}

// MonitoringConfig defines monitoring and logging configuration
type MonitoringConfig struct {
	Enabled          bool              `json:"enabled,omitempty"`
	MetricsPort      int               `json:"metricsPort,omitempty"`
	LogLevel         string            `json:"logLevel,omitempty"`
	PrometheusLabels map[string]string `json:"prometheusLabels,omitempty"`
}

// RetryPolicy defines retry behavior
type RetryPolicy struct {
	MaxRetries         int  `json:"maxRetries,omitempty"`
	RetryInterval      int  `json:"retryInterval,omitempty"`
	ExponentialBackoff bool `json:"exponentialBackoff,omitempty"`
}

// InstorageJobStatus defines the observed state of InstorageJob
type InstorageJobStatus struct {
	Phase            JobPhase              `json:"phase,omitempty"`
	StartTime        *metav1.Time          `json:"startTime,omitempty"`
	CompletionTime   *metav1.Time          `json:"completionTime,omitempty"`
	ProcessedRecords int                   `json:"processedRecords,omitempty"`
	TotalRecords     int                   `json:"totalRecords,omitempty"`
	CSDUtilization   float64               `json:"csdUtilization,omitempty"`
	Message          string                `json:"message,omitempty"`
	TargetNode       string                `json:"targetNode,omitempty"`
	BatchExecution   *BatchExecutionStatus `json:"batchExecution,omitempty"`
	DataAnalysis     *DataAnalysisResult   `json:"dataAnalysis,omitempty"`
	Conditions       []JobCondition        `json:"conditions,omitempty"`
}

// BatchExecutionStatus defines batch execution status
type BatchExecutionStatus struct {
	TotalBatches         int           `json:"totalBatches,omitempty"`
	CompletedBatches     int           `json:"completedBatches,omitempty"`
	FailedBatches        int           `json:"failedBatches,omitempty"`
	RunningBatches       int           `json:"runningBatches,omitempty"`
	ActiveBatches        []BatchStatus `json:"activeBatches,omitempty"`
	BatchStrategy        string        `json:"batchStrategy,omitempty"`
	AverageBatchDuration int           `json:"averageBatchDuration,omitempty"`
	EstimatedCompletion  *metav1.Time  `json:"estimatedCompletion,omitempty"`
}

// BatchStatus defines individual batch status
type BatchStatus struct {
	BatchId        string       `json:"batchId,omitempty"`
	Status         string       `json:"status,omitempty"`
	StartTime      *metav1.Time `json:"startTime,omitempty"`
	ItemCount      int          `json:"itemCount,omitempty"`
	ProcessedItems int          `json:"processedItems,omitempty"`
	Message        string       `json:"message,omitempty"`
}

// DataAnalysisResult defines data analysis result
type DataAnalysisResult struct {
	TotalFiles     int            `json:"totalFiles,omitempty"`
	TotalSizeBytes int64          `json:"totalSizeBytes,omitempty"`
	AvgFileSize    int64          `json:"avgFileSize,omitempty"`
	FileTypes      map[string]int `json:"fileTypes,omitempty"`
	DiscoveryTime  *metav1.Time   `json:"discoveryTime,omitempty"`
	SampledFiles   int            `json:"sampledFiles,omitempty"`
}

// JobCondition defines job condition
type JobCondition struct {
	Type               string      `json:"type"`
	Status             string      `json:"status"`
	LastTransitionTime metav1.Time `json:"lastTransitionTime"`
	Reason             string      `json:"reason,omitempty"`
	Message            string      `json:"message,omitempty"`
}

type JobPhase string

const (
	JobPhasePending   JobPhase = "Pending"
	JobPhaseRunning   JobPhase = "Running"
	JobPhaseSucceeded JobPhase = "Succeeded"
	JobPhaseFailed    JobPhase = "Failed"
	JobPhaseUnknown   JobPhase = "Unknown"
)

// DeepCopyObject returns a generically typed copy of an object
func (in *InstorageJob) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new InstorageJob.
func (in *InstorageJob) DeepCopy() *InstorageJob {
	if in == nil {
		return nil
	}
	out := new(InstorageJob)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *InstorageJob) DeepCopyInto(out *InstorageJob) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}

// DeepCopyObject returns a generically typed copy of an object
func (in *InstorageJobList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new InstorageJobList.
func (in *InstorageJobList) DeepCopy() *InstorageJobList {
	if in == nil {
		return nil
	}
	out := new(InstorageJobList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *InstorageJobList) DeepCopyInto(out *InstorageJobList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]InstorageJob, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *InstorageJobSpec) DeepCopyInto(out *InstorageJobSpec) {
	*out = *in
	if in.CSD != nil {
		in, out := &in.CSD, &out.CSD
		*out = new(CSDConfig)
		(*in).DeepCopyInto(*out)
	}
	if in.DataLocations != nil {
		in, out := &in.DataLocations, &out.DataLocations
		*out = new(DataLocations)
		(*in).DeepCopyInto(*out)
	}
	if in.Preprocessing != nil {
		in, out := &in.Preprocessing, &out.Preprocessing
		*out = new(PreprocessingConfig)
		(*in).DeepCopyInto(*out)
	}
	if in.Resources != nil {
		in, out := &in.Resources, &out.Resources
		*out = new(ResourceConfig)
		(*in).DeepCopyInto(*out)
	}
	if in.NodeScheduling != nil {
		in, out := &in.NodeScheduling, &out.NodeScheduling
		*out = new(NodeScheduling)
		(*in).DeepCopyInto(*out)
	}
	if in.JobConfig != nil {
		in, out := &in.JobConfig, &out.JobConfig
		*out = new(JobConfig)
		**out = **in
	}
	if in.Monitoring != nil {
		in, out := &in.Monitoring, &out.Monitoring
		*out = new(MonitoringConfig)
		(*in).DeepCopyInto(*out)
	}
	if in.RetryPolicy != nil {
		in, out := &in.RetryPolicy, &out.RetryPolicy
		*out = new(RetryPolicy)
		**out = **in
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new InstorageJobSpec.
func (in *InstorageJobSpec) DeepCopy() *InstorageJobSpec {
	if in == nil {
		return nil
	}
	out := new(InstorageJobSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *InstorageJobStatus) DeepCopyInto(out *InstorageJobStatus) {
	*out = *in
	if in.StartTime != nil {
		in, out := &in.StartTime, &out.StartTime
		*out = (*in).DeepCopy()
	}
	if in.CompletionTime != nil {
		in, out := &in.CompletionTime, &out.CompletionTime
		*out = (*in).DeepCopy()
	}
	if in.Conditions != nil {
		in, out := &in.Conditions, &out.Conditions
		*out = make([]JobCondition, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new InstorageJobStatus.
func (in *InstorageJobStatus) DeepCopy() *InstorageJobStatus {
	if in == nil {
		return nil
	}
	out := new(InstorageJobStatus)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *CSDConfig) DeepCopyInto(out *CSDConfig) {
	*out = *in
	if in.OffloadingRatio != nil {
		in, out := &in.OffloadingRatio, &out.OffloadingRatio
		*out = new(OffloadingRatio)
		**out = **in
	}
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *DataLocations) DeepCopyInto(out *DataLocations) {
	*out = *in
	if in.Locations != nil {
		in, out := &in.Locations, &out.Locations
		*out = make([]string, len(*in))
		copy(*out, *in)
	}
	if in.Weights != nil {
		in, out := &in.Weights, &out.Weights
		*out = make([]LocationWeight, len(*in))
		copy(*out, *in)
	}
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *ResourceConfig) DeepCopyInto(out *ResourceConfig) {
	*out = *in
	if in.Requests != nil {
		in, out := &in.Requests, &out.Requests
		*out = new(ResourceList)
		**out = **in
	}
	if in.Limits != nil {
		in, out := &in.Limits, &out.Limits
		*out = new(ResourceList)
		**out = **in
	}
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *NodeScheduling) DeepCopyInto(out *NodeScheduling) {
	*out = *in
	if in.NodeSelector != nil {
		in, out := &in.NodeSelector, &out.NodeSelector
		*out = make(map[string]string, len(*in))
		for key, val := range *in {
			(*out)[key] = val
		}
	}
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *MonitoringConfig) DeepCopyInto(out *MonitoringConfig) {
	*out = *in
	if in.PrometheusLabels != nil {
		in, out := &in.PrometheusLabels, &out.PrometheusLabels
		*out = make(map[string]string, len(*in))
		for key, val := range *in {
			(*out)[key] = val
		}
	}
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *JobCondition) DeepCopyInto(out *JobCondition) {
	*out = *in
	in.LastTransitionTime.DeepCopyInto(&out.LastTransitionTime)
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *PreprocessingConfig) DeepCopyInto(out *PreprocessingConfig) {
	*out = *in
	if in.BatchExecution != nil {
		in, out := &in.BatchExecution, &out.BatchExecution
		*out = new(BatchExecution)
		**out = **in
	}
	if in.DataDiscovery != nil {
		in, out := &in.DataDiscovery, &out.DataDiscovery
		*out = new(DataDiscovery)
		(*in).DeepCopyInto(*out)
	}
	if in.DynamicScaling != nil {
		in, out := &in.DynamicScaling, &out.DynamicScaling
		*out = new(DynamicScaling)
		**out = **in
	}
	if in.Optimization != nil {
		in, out := &in.Optimization, &out.Optimization
		*out = new(Optimization)
		**out = **in
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new PreprocessingConfig.
func (in *PreprocessingConfig) DeepCopy() *PreprocessingConfig {
	if in == nil {
		return nil
	}
	out := new(PreprocessingConfig)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *DataDiscovery) DeepCopyInto(out *DataDiscovery) {
	*out = *in
	if in.FileExtensions != nil {
		in, out := &in.FileExtensions, &out.FileExtensions
		*out = make([]string, len(*in))
		copy(*out, *in)
	}
	if in.ExcludePatterns != nil {
		in, out := &in.ExcludePatterns, &out.ExcludePatterns
		*out = make([]string, len(*in))
		copy(*out, *in)
	}
	if in.UserProvidedData != nil {
		in, out := &in.UserProvidedData, &out.UserProvidedData
		*out = new(UserProvidedData)
		(*in).DeepCopyInto(*out)
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new DataDiscovery.
func (in *DataDiscovery) DeepCopy() *DataDiscovery {
	if in == nil {
		return nil
	}
	out := new(DataDiscovery)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *UserProvidedData) DeepCopyInto(out *UserProvidedData) {
	*out = *in
	if in.FileTypes != nil {
		in, out := &in.FileTypes, &out.FileTypes
		*out = make(map[string]int, len(*in))
		for key, val := range *in {
			(*out)[key] = val
		}
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new UserProvidedData.
func (in *UserProvidedData) DeepCopy() *UserProvidedData {
	if in == nil {
		return nil
	}
	out := new(UserProvidedData)
	in.DeepCopyInto(out)
	return out
}

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&InstorageJob{},
		&InstorageJobList{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
