// Package v1alpha1 contains the initial regional PostgresCluster API.
// +kubebuilder:object:generate=true
// +groupName=postgres.example.com
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	GroupVersion  = schema.GroupVersion{Group: "postgres.example.com", Version: "v1alpha1"}
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}
	AddToScheme   = SchemeBuilder.AddToScheme
)
