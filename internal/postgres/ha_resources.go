package postgres

import (
	"sort"

	pgv1 "github.com/vsamidurai/PostgresManager/api/v1alpha1"
	"github.com/vsamidurai/PostgresManager/internal/ha"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func HAControlName(c *pgv1.PostgresCluster) string { return ResourceNames(c).Control }
func ObservationName(instance string) string       { return instance + "-status" }

func HALabels(c *pgv1.PostgresCluster, instance string) map[string]string {
	labels := Labels(c)
	labels[InstanceLabel] = instance
	return labels
}

func HAPVC(c *pgv1.PostgresCluster, instance string) *corev1.PersistentVolumeClaim {
	p := PVC(c)
	p.Name = instance + "-data"
	p.Labels = HALabels(c, instance)
	return p
}

func HAPod(c *pgv1.PostgresCluster, member ha.Member, controlUID string) *corev1.Pod {
	p := Pod(c)
	p.Name = member.Name
	p.Labels = HALabels(c, member.Name)
	p.Spec.Hostname = member.Name
	p.Spec.ServiceAccountName = member.Name
	p.Spec.AutomountServiceAccountToken = ptr.To(true)
	p.Spec.TerminationGracePeriodSeconds = ptr.To(int64(60))
	p.Spec.Affinity = &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{TopologyKey: "kubernetes.io/hostname", LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{ClusterUIDLabel: string(c.UID)}}}}}}
	container := &p.Spec.Containers[0]
	container.Command = []string{"/usr/local/bin/postgres-manager-instance"}
	container.Args = nil
	keys := []string{}
	for k := range c.Spec.Postgres.Parameters {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		container.Args = append(container.Args, "--parameter", k+"="+string(c.Spec.Postgres.Parameters[k]))
	}
	container.Env = []corev1.EnvVar{
		{Name: "PGDATA", Value: "/var/lib/postgresql/data/pgdata"},
		{Name: "PGM_INSTANCE", Value: member.Name}, {Name: "PGM_CLUSTER_UID", Value: string(c.UID)},
		{Name: "PGM_CONTROL", Value: HAControlName(c)}, {Name: "PGM_CONTROL_UID", Value: controlUID},
		{Name: "PGM_PVC_UID", Value: member.PVCUID}, {Name: "PGM_HEADLESS", Value: ResourceNames(c).Headless},
		{Name: "PGM_NAMESPACE", Value: c.Namespace}, {Name: "PGM_MODE", Value: c.Spec.Replication.Mode},
		{Name: "PGM_POD_UID", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.uid"}}},
	}
	container.Ports = append(container.Ports, corev1.ContainerPort{Name: "management", ContainerPort: 8080})
	container.StartupProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/health", Port: intstr.FromInt32(8080)}}, PeriodSeconds: 2, TimeoutSeconds: 2, FailureThreshold: 60}
	container.ReadinessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/ready", Port: intstr.FromInt32(8080)}}, PeriodSeconds: 2, TimeoutSeconds: 2, FailureThreshold: 1}
	container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{Name: "replication", MountPath: "/etc/postgres-replication", ReadOnly: true}, corev1.VolumeMount{Name: "tls", MountPath: "/etc/postgres-tls", ReadOnly: true})
	for i := range p.Spec.Volumes {
		if p.Spec.Volumes[i].Name == "data" {
			p.Spec.Volumes[i].PersistentVolumeClaim.ClaimName = member.Name + "-data"
		}
	}
	p.Spec.Volumes = append(p.Spec.Volumes,
		corev1.Volume{Name: "replication", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: c.Spec.Replication.CredentialsSecretRef.Name, DefaultMode: ptr.To(int32(0440)), Items: []corev1.KeyToPath{{Key: "password", Path: "password"}}}}},
		corev1.Volume{Name: "tls", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: c.Spec.Replication.TLSSecretRef.Name, DefaultMode: ptr.To(int32(0440))}}})
	return p
}

func HAIdentityObjects(c *pgv1.PostgresCluster, instance string) []client.Object {
	meta := func() metav1.ObjectMeta {
		return metav1.ObjectMeta{Name: instance, Namespace: c.Namespace, Labels: HALabels(c, instance)}
	}
	return []client.Object{
		&corev1.ServiceAccount{ObjectMeta: meta(), AutomountServiceAccountToken: ptr.To(false)},
		&rbacv1.Role{ObjectMeta: meta(), Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{""}, Resources: []string{"configmaps"}, ResourceNames: []string{HAControlName(c), ObservationName(instance)}, Verbs: []string{"get"}},
			{APIGroups: []string{""}, Resources: []string{"configmaps"}, ResourceNames: []string{ObservationName(instance)}, Verbs: []string{"update"}},
		}},
		&rbacv1.RoleBinding{ObjectMeta: meta(), RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: instance}, Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: instance, Namespace: c.Namespace}}},
	}
}
