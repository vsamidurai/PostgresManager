package controller

import (
	"testing"
	"time"

	pgv1 "github.com/vsamidurai/PostgresManager/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

func TestClusterPredicateKeepsLifecycleEvents(t *testing.T) {
	p := clusterPredicate()
	before := &pgv1.PostgresCluster{ObjectMeta: metav1.ObjectMeta{Generation: 1}}
	if !p.Create(event.CreateEvent{Object: before}) || !p.Delete(event.DeleteEvent{Object: before}) {
		t.Fatal("create and delete events must reconcile")
	}
	for _, tc := range []struct {
		name   string
		change func(*pgv1.PostgresCluster)
		want   bool
	}{
		{"status", func(c *pgv1.PostgresCluster) { c.Status.ReadyInstances = 1 }, false},
		{"spec", func(c *pgv1.PostgresCluster) { c.Generation++ }, true},
		{"finalization", func(c *pgv1.PostgresCluster) { c.Finalizers = []string{finalizer} }, true},
		{"deletion", func(c *pgv1.PostgresCluster) { stamp := metav1.NewTime(time.Now()); c.DeletionTimestamp = &stamp }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			after := before.DeepCopy()
			tc.change(after)
			if got := p.Update(event.UpdateEvent{ObjectOld: before, ObjectNew: after}); got != tc.want {
				t.Fatalf("event accepted = %v, want %v", got, tc.want)
			}
		})
	}
}
