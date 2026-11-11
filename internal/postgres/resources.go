// Package postgres builds resources for the bootstrap-only Postgres component.
package postgres

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"unicode/utf8"

	pgv1 "github.com/vsamidurai/PostgresManager/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
)

const (
	ClusterLabel    = "postgres.example.com/cluster"
	ClusterUIDLabel = "postgres.example.com/cluster-uid"
	InstanceLabel   = "postgres.example.com/instance"
	ManagedByLabel  = "app.kubernetes.io/managed-by"
	ManagedByValue  = "postgres-manager"
)

var allowedParameters = map[string]bool{
	"max_connections": true, "shared_buffers": true, "work_mem": true,
	"maintenance_work_mem": true, "effective_cache_size": true,
	"log_min_duration_statement": true, "log_statement": true, "log_connections": true,
}

// Names bind storage to a CR incarnation, not a reusable display name or role.
type Names struct{ Instance, PVC, Control, Writer, Headless string }

func ResourceNames(c *pgv1.PostgresCluster) Names {
	return InstanceNames(c, 1)
}

func InstanceNames(c *pgv1.PostgresCluster, ordinal int) Names {
	sum := sha256.Sum256([]byte(c.UID))
	id := c.Name + "-" + hex.EncodeToString(sum[:5]) + "-" + strconv.Itoa(ordinal)
	return Names{Instance: id, PVC: id + "-data", Control: id + "-control", Writer: c.Name + "-rw", Headless: c.Name + "-instances"}
}

func Labels(c *pgv1.PostgresCluster) map[string]string {
	return map[string]string{ClusterLabel: c.Name, ClusterUIDLabel: string(c.UID), InstanceLabel: ResourceNames(c).Instance, ManagedByLabel: ManagedByValue}
}

func ContractHash(c *pgv1.PostgresCluster) string {
	spec := c.Spec.DeepCopy()
	spec.Storage.Data.Size = "" // Capacity is the only mutable field in this slice.
	if spec.Replication != nil {
		spec.Instances = 0
	} // HA membership is journaled separately.
	b, _ := json.Marshal(spec)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func Validate(c *pgv1.PostgresCluster) error {
	if c.UID == "" || len(c.Name) > 40 {
		return fmt.Errorf("cluster must have a UID and a name of at most 40 characters")
	}
	if c.Spec.Replication == nil && c.Spec.Instances != 1 {
		return fmt.Errorf("multiple instances require the managed replication profile")
	}
	if p := c.Spec.Replication; p != nil {
		if c.Spec.Bootstrap.InitDB.Checksums != nil && !*c.Spec.Bootstrap.InitDB.Checksums {
			return fmt.Errorf("managed replication requires data checksums")
		}
		if c.Spec.Instances < 2 || c.Spec.Instances > 5 || (p.Mode != "Synchronous" && p.Mode != "Asynchronous") || (p.DataLossPolicy != "Lossless" && p.DataLossPolicy != "AllowPotentialDataLoss") || p.CredentialsSecretRef.Name == "" || p.TLSSecretRef.Name == "" {
			return fmt.Errorf("invalid managed replication profile")
		}
	}
	if c.Spec.Postgres.MajorVersion != 17 || c.Spec.Postgres.Image == "" {
		return fmt.Errorf("a PostgreSQL 17 image is required")
	}
	if c.Spec.Bootstrap.InitDB.SuperuserSecretRef.Name == "" {
		return fmt.Errorf("bootstrap superuser Secret reference is required")
	}
	if c.Spec.DeletionPolicy != "Retain" {
		return fmt.Errorf("only Retain deletion policy is supported")
	}
	if mode := c.Spec.Storage.Data.AccessMode; mode != corev1.ReadWriteOnce && mode != corev1.ReadWriteOncePod {
		return fmt.Errorf("unsupported storage access mode")
	}
	q, err := resource.ParseQuantity(c.Spec.Storage.Data.Size)
	if err != nil || q.Sign() <= 0 {
		return fmt.Errorf("storage size must be a positive Kubernetes quantity")
	}
	for key, value := range c.Spec.Postgres.Parameters {
		if !allowedParameters[key] {
			return fmt.Errorf("unsupported or protected Postgres parameter %q", key)
		}
		if utf8.RuneCountInString(string(value)) > 1024 {
			return fmt.Errorf("postgres parameter %q exceeds 1024 characters", key)
		}
	}
	return nil
}

// PVCs deliberately have no owner reference: deleting a CR must retain data.
func PVC(c *pgv1.PostgresCluster) *corev1.PersistentVolumeClaim {
	var storageClass *string
	if c.Spec.Storage.Data.StorageClassName != "" {
		storageClass = ptr.To(c.Spec.Storage.Data.StorageClassName)
	}
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: ResourceNames(c).PVC, Namespace: c.Namespace, Labels: Labels(c)},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{c.Spec.Storage.Data.AccessMode},
			StorageClassName: storageClass, VolumeMode: ptr.To(corev1.PersistentVolumeFilesystem),
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(c.Spec.Storage.Data.Size)}},
		},
	}
}

// The guard prevents adopting unmarked data or using a different major binary.
// It is a bootstrap guard, not an HA authority/fencing implementation.
const startupGuard = `set -eu
actual_major=$(postgres --version | awk '{split($3, version, "."); print version[1]}')
if [ "$actual_major" != "$POSTGRES_MAJOR" ]; then
  echo "Postgres image major version does not match the cluster specification" >&2; exit 1
fi
if [ -f "$PGDATA/PG_VERSION" ] && [ "$(cat "$PGDATA/PG_VERSION")" != "$POSTGRES_MAJOR" ]; then
  echo "Existing data requires a controlled major upgrade" >&2; exit 1
fi
marker=/var/lib/postgresql/data/.postgres-manager-instance
if [ -f "$marker" ]; then
  if [ "$(cat "$marker")" != "$POSTGRES_INSTANCE_ID" ]; then
    echo "Data volume belongs to another Postgres instance" >&2; exit 1
  fi
else
  if [ -n "$(find /var/lib/postgresql/data -mindepth 1 -maxdepth 1 ! -name lost+found -print -quit)" ]; then
    echo "Refusing to initialize or adopt an unmarked nonempty data volume" >&2; exit 1
  fi
  umask 077
  printf '%s\n' "$POSTGRES_INSTANCE_ID" > "$marker.tmp"
  mv "$marker.tmp" "$marker"
fi
exec docker-entrypoint.sh postgres "$@"
`

func Pod(c *pgv1.PostgresCluster) *corev1.Pod {
	n := ResourceNames(c)
	args := []string{startupGuard, "postgres-manager", "-c", "password_encryption=scram-sha-256"}
	keys := make([]string, 0, len(c.Spec.Postgres.Parameters))
	for key := range c.Spec.Postgres.Parameters {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		args = append(args, "-c", key+"="+string(c.Spec.Postgres.Parameters[key]))
	}
	initArgs := "--auth-host=scram-sha-256"
	if c.Spec.Bootstrap.InitDB.Checksums == nil || *c.Spec.Bootstrap.InitDB.Checksums {
		initArgs += " --data-checksums"
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: n.Instance, Namespace: c.Namespace, Labels: Labels(c)},
		Spec: corev1.PodSpec{
			AutomountServiceAccountToken: ptr.To(false), Hostname: n.Instance, Subdomain: n.Headless,
			RestartPolicy: corev1.RestartPolicyAlways, TerminationGracePeriodSeconds: ptr.To(int64(60)),
			SecurityContext: &corev1.PodSecurityContext{RunAsNonRoot: ptr.To(true), RunAsUser: ptr.To(int64(999)), RunAsGroup: ptr.To(int64(999)), FSGroup: ptr.To(int64(999)), SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}},
			Containers: []corev1.Container{{
				Name: "postgres", Image: c.Spec.Postgres.Image, ImagePullPolicy: corev1.PullIfNotPresent,
				Command: []string{"/bin/sh", "-c"}, Args: args,
				Env: []corev1.EnvVar{
					{Name: "PGDATA", Value: "/var/lib/postgresql/data/pgdata"},
					{Name: "POSTGRES_USER", Value: "postgres"}, {Name: "POSTGRES_DB", Value: "postgres"},
					{Name: "POSTGRES_PASSWORD_FILE", Value: "/etc/postgres-bootstrap/password"},
					{Name: "POSTGRES_INITDB_ARGS", Value: initArgs}, {Name: "POSTGRES_HOST_AUTH_METHOD", Value: "scram-sha-256"},
					{Name: "POSTGRES_INSTANCE_ID", Value: n.Instance}, {Name: "POSTGRES_MAJOR", Value: strconv.Itoa(int(c.Spec.Postgres.MajorVersion))},
				},
				Ports: []corev1.ContainerPort{{Name: "postgres", ContainerPort: 5432}}, Resources: *c.Spec.Resources.DeepCopy(),
				SecurityContext: &corev1.SecurityContext{AllowPrivilegeEscalation: ptr.To(false), ReadOnlyRootFilesystem: ptr.To(true), Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}},
				VolumeMounts:    []corev1.VolumeMount{{Name: "data", MountPath: "/var/lib/postgresql/data"}, {Name: "socket", MountPath: "/var/run/postgresql"}, {Name: "tmp", MountPath: "/tmp"}, {Name: "bootstrap", MountPath: "/etc/postgres-bootstrap", ReadOnly: true}},
				StartupProbe:    &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"pg_isready", "-h", "127.0.0.1", "-U", "postgres", "-d", "postgres"}}}, PeriodSeconds: 5, TimeoutSeconds: 3, FailureThreshold: 120},
				ReadinessProbe:  &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"sh", "-ec", `pg_isready -h 127.0.0.1 -U postgres -d postgres >/dev/null && test "$(psql -U postgres -d postgres -Atqc 'SELECT NOT pg_is_in_recovery()')" = t`}}}, PeriodSeconds: 5, TimeoutSeconds: 3, FailureThreshold: 3},
			}},
			Volumes: []corev1.Volume{
				{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: n.PVC}}},
				{Name: "socket", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
				{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
				{Name: "bootstrap", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: c.Spec.Bootstrap.InitDB.SuperuserSecretRef.Name, DefaultMode: ptr.To(int32(0440)), Items: []corev1.KeyToPath{{Key: "password", Path: "password"}}}}},
			},
		},
	}
}

func Service(c *pgv1.PostgresCluster, headless bool) *corev1.Service {
	n := ResourceNames(c)
	s := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: n.Writer, Namespace: c.Namespace, Labels: Labels(c)}, Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP, Selector: map[string]string{ClusterUIDLabel: string(c.UID), InstanceLabel: n.Instance}, Ports: []corev1.ServicePort{{Name: "postgres", Port: 5432, TargetPort: intstr.FromString("postgres"), Protocol: corev1.ProtocolTCP}}}}
	if headless {
		s.Name = n.Headless
		s.Spec.ClusterIP = corev1.ClusterIPNone
	}
	return s
}
