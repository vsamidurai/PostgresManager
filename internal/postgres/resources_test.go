package postgres

import (
	"strings"
	"testing"

	pgv1 "github.com/vsamidurai/PostgresManager/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func cluster() *pgv1.PostgresCluster {
	return &pgv1.PostgresCluster{ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "databases", UID: "first-incarnation"}, Spec: pgv1.PostgresClusterSpec{
		Region: "local", Instances: 1, Postgres: pgv1.PostgresSpec{MajorVersion: 17, Image: "postgres:17-bookworm"},
		Bootstrap: pgv1.BootstrapSpec{InitDB: pgv1.InitDBSpec{SuperuserSecretRef: pgv1.SecretReference{Name: "bootstrap"}}},
		Storage:   pgv1.StorageSpec{Data: pgv1.DataStorageSpec{Size: "1Gi", AccessMode: corev1.ReadWriteOnce}}, DeletionPolicy: "Retain",
	}}
}

func TestRecreatedClusterCannotReuseOldStorage(t *testing.T) {
	first := cluster()
	second := first.DeepCopy()
	second.UID = "second-incarnation"
	if ResourceNames(first).PVC == ResourceNames(second).PVC {
		t.Fatal("reusing a cluster name must not reuse its old data PVC")
	}
	if len(PVC(first).OwnerReferences) != 0 {
		t.Fatal("retained data must not be garbage-collected with the cluster")
	}
}

func TestOnlyCapacityCanChangeWithoutChangingBootstrapContract(t *testing.T) {
	c := cluster()
	original := ContractHash(c)
	c.Spec.Storage.Data.Size = "2Gi"
	if ContractHash(c) != original {
		t.Fatal("capacity expansion must not replace Postgres")
	}
	c.Spec.Postgres.Image = "postgres:18-bookworm"
	if ContractHash(c) == original {
		t.Fatal("image changes must not reuse the bootstrap authorization")
	}
}

func TestProtectedSettingsAndReplicationAreRejected(t *testing.T) {
	for _, key := range []string{"archive_command", "shared_preload_libraries", "data_directory", "synchronous_standby_names", "listen_addresses"} {
		t.Run(key, func(t *testing.T) {
			c := cluster()
			c.Spec.Postgres.Parameters = map[string]string{key: "unsafe"}
			if Validate(c) == nil {
				t.Fatal("protected setting was accepted")
			}
		})
	}
	c := cluster()
	c.Spec.Instances = 2
	if Validate(c) == nil {
		t.Fatal("multiple independent writable instances must not be created")
	}
}

func TestParameterValuesCannotBecomeShellCommands(t *testing.T) {
	c := cluster()
	injection := "$(touch /tmp/escaped); echo unsafe"
	c.Spec.Postgres.Parameters = map[string]string{"log_statement": injection}
	pod := Pod(c)
	args := pod.Spec.Containers[0].Args
	if strings.Contains(args[0], injection) {
		t.Fatal("parameter was interpolated into the shell program")
	}
	if args[len(args)-1] != "log_statement="+injection {
		t.Fatal("parameter must remain one literal postgres argument")
	}
	if pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken {
		t.Fatal("database does not need Kubernetes API credentials")
	}
	for _, env := range pod.Spec.Containers[0].Env {
		if env.Name == "POSTGRES_PASSWORD" {
			t.Fatal("bootstrap credentials must be read from a Secret file")
		}
	}
}
