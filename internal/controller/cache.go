package controller

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/vsamidurai/PostgresManager/internal/postgres"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
)

// CacheOptions bounds watch memory to managed resources. Secrets and storage
// classes must use the APIReader, so accidental cache reads fail explicitly.
func CacheOptions(namespace string) cache.Options {
	managed := labels.SelectorFromSet(labels.Set{postgres.ManagedByLabel: postgres.ManagedByValue})
	options := cache.Options{
		ReaderFailOnMissingInformer: true,
		DefaultTransform:            cache.TransformStripManagedFields(),
		ByObject: map[client.Object]cache.ByObject{
			&corev1.Pod{}:                   {Label: managed},
			&corev1.PersistentVolumeClaim{}: {Label: managed},
			&corev1.Service{}:               {Label: managed},
			&corev1.ConfigMap{}:             {Label: managed},
		},
	}
	if namespace != "" {
		options.DefaultNamespaces = map[string]cache.Config{namespace: {}}
	}
	return options
}

// CacheReady does not require leadership: healthy standby managers must remain
// ready during rolling deployments. Each probe has a bounded wait for sync.
func CacheReady(c cache.Cache) healthz.Checker {
	return func(req *http.Request) error {
		ctx, cancel := context.WithTimeout(req.Context(), time.Second)
		defer cancel()
		if !c.WaitForCacheSync(ctx) {
			return fmt.Errorf("resource cache has not synced")
		}
		return nil
	}
}
