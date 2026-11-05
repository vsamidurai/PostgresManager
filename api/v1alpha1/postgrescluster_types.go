package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PostgresClusterSpec is deliberately limited to a single bootstrap-only server.
type PostgresClusterSpec struct {
	// Region is descriptive in this regional implementation.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="region is immutable"
	Region string `json:"region"`
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=1
	Instances int32         `json:"instances,omitempty"`
	Postgres  PostgresSpec  `json:"postgres"`
	Bootstrap BootstrapSpec `json:"bootstrap"`
	Storage   StorageSpec   `json:"storage"`
	// Resources are fixed at bootstrap until controlled replacement is implemented.
	// +kubebuilder:default={}
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="resource changes require a future replacement operation"
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
	// +kubebuilder:default=Retain
	// +kubebuilder:validation:Enum=Retain
	DeletionPolicy string `json:"deletionPolicy,omitempty"`
}

// PostgresSpec describes the supported PostgreSQL 17 Debian image contract.
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="Postgres configuration is immutable until update operations are implemented"
type PostgresSpec struct {
	// +kubebuilder:default=17
	// +kubebuilder:validation:Enum=17
	MajorVersion int32 `json:"majorVersion,omitempty"`
	// Image must implement the official postgres:17-bookworm entrypoint and UID 999.
	// +kubebuilder:default="postgres:17-bookworm"
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=512
	Image string `json:"image,omitempty"`
	// Parameters are supplied as individual postgres arguments, never shell code.
	// +kubebuilder:validation:MaxProperties=8
	// +kubebuilder:validation:XValidation:rule="self.all(k, k in ['max_connections', 'shared_buffers', 'work_mem', 'maintenance_work_mem', 'effective_cache_size', 'log_min_duration_statement', 'log_statement', 'log_connections'])",message="unsupported or protected Postgres parameter"
	Parameters map[string]string `json:"parameters,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="bootstrap is immutable"
type BootstrapSpec struct {
	InitDB InitDBSpec `json:"initdb"`
}

type InitDBSpec struct {
	// SuperuserSecretRef references a same-namespace Secret with a password key.
	SuperuserSecretRef SecretReference `json:"superuserSecretRef"`
	// +kubebuilder:default=true
	Checksums *bool `json:"checksums,omitempty"`
}

type SecretReference struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`
}

type StorageSpec struct {
	Data DataStorageSpec `json:"data"`
}

type DataStorageSpec struct {
	// Empty means use the cluster's default StorageClass.
	// +kubebuilder:default=""
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="storageClassName is immutable"
	StorageClassName string `json:"storageClassName,omitempty"`
	// Size accepts Kubernetes quantity syntax; only growth is permitted.
	// +kubebuilder:validation:MaxLength=32
	// +kubebuilder:validation:XValidation:rule="isQuantity(self) && quantity(self).isGreaterThan(quantity('0'))",message="size must be a positive Kubernetes quantity"
	// +kubebuilder:validation:XValidation:rule="quantity(self).compareTo(quantity(oldSelf)) >= 0",message="storage cannot shrink"
	Size string `json:"size"`
	// +kubebuilder:default=ReadWriteOnce
	// +kubebuilder:validation:Enum=ReadWriteOnce;ReadWriteOncePod
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="accessMode is immutable"
	AccessMode corev1.PersistentVolumeAccessMode `json:"accessMode,omitempty"`
}

type PostgresClusterStatus struct {
	ObservedGeneration int64  `json:"observedGeneration,omitempty"`
	InstanceID         string `json:"instanceID,omitempty"`
	PodName            string `json:"podName,omitempty"`
	PodUID             string `json:"podUID,omitempty"`
	PVCName            string `json:"pvcName,omitempty"`
	PVCUID             string `json:"pvcUID,omitempty"`
	ReadyInstances     int32  `json:"readyInstances,omitempty"`
	Endpoint           string `json:"endpoint,omitempty"`
	VolumeCapacity     string `json:"volumeCapacity,omitempty"`
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=pgc
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=".status.readyInstances"
// +kubebuilder:printcolumn:name="Endpoint",type=string,JSONPath=".status.endpoint"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"
// +kubebuilder:validation:XValidation:rule="self.metadata.name.size() <= 40 && self.metadata.name.matches('^[a-z0-9]([-a-z0-9]*[a-z0-9])?$')",message="name must be a DNS label of at most 40 characters"
type PostgresCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              PostgresClusterSpec   `json:"spec"`
	Status            PostgresClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type PostgresClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PostgresCluster `json:"items"`
}

func init() { SchemeBuilder.Register(&PostgresCluster{}, &PostgresClusterList{}) }
