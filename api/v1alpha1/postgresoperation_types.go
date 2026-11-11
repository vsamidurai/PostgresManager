package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// PostgresOperationSpec is an immutable, incarnation-bound regional request.
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="operations are immutable"
type PostgresOperationSpec struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=40
	ClusterName string `json:"clusterName"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=64
	ClusterUID string `json:"clusterUID"`
	// +kubebuilder:validation:Enum=Switchover;Rejoin
	Type string `json:"type"`
	// TargetInstance is the immutable instance name from cluster status.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	TargetInstance string `json:"targetInstance"`
}

type PostgresOperationStatus struct {
	Phase              string `json:"phase,omitempty"`
	Message            string `json:"message,omitempty"`
	ObservedGeneration int64  `json:"observedGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=pgop
type PostgresOperation struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              PostgresOperationSpec   `json:"spec"`
	Status            PostgresOperationStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type PostgresOperationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PostgresOperation `json:"items"`
}

func init() { SchemeBuilder.Register(&PostgresOperation{}, &PostgresOperationList{}) }
