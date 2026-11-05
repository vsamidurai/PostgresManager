package main

import (
	"flag"
	"os"

	pgv1 "github.com/vsamidurai/PostgresManager/api/v1alpha1"
	"github.com/vsamidurai/PostgresManager/internal/controller"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

func main() {
	var namespace, probeAddress, metricsAddress, electionNamespace string
	var leaderElection bool
	flag.StringVar(&namespace, "watch-namespace", "", "Namespace to reconcile; empty watches all namespaces")
	flag.StringVar(&electionNamespace, "leader-election-namespace", "postgres-manager-system", "Namespace containing the regional controller lease")
	flag.StringVar(&probeAddress, "health-probe-bind-address", ":8081", "Health probe listen address")
	flag.StringVar(&metricsAddress, "metrics-bind-address", "0", "Metrics listen address; 0 disables metrics")
	flag.BoolVar(&leaderElection, "leader-elect", true, "Elect one active regional controller")
	logging := zap.Options{}
	logging.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&logging)))
	scheme := runtime.NewScheme()
	must(clientgoscheme.AddToScheme(scheme))
	must(pgv1.AddToScheme(scheme))
	options := ctrl.Options{Scheme: scheme, Metrics: metricsserver.Options{BindAddress: metricsAddress}, HealthProbeBindAddress: probeAddress, LeaderElection: leaderElection, LeaderElectionID: "postgres-manager-regional.postgres.example.com", LeaderElectionNamespace: electionNamespace}
	if namespace != "" {
		options.Cache = cache.Options{DefaultNamespaces: map[string]cache.Config{namespace: {}}}
	}
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), options)
	must(err)
	r := &controller.PostgresClusterReconciler{Client: mgr.GetClient(), APIReader: mgr.GetAPIReader(), Scheme: scheme}
	must(r.SetupWithManager(mgr))
	must(mgr.AddHealthzCheck("healthz", healthz.Ping))
	must(mgr.AddReadyzCheck("readyz", healthz.Ping))
	must(mgr.Start(ctrl.SetupSignalHandler()))
}

func must(err error) {
	if err != nil {
		ctrl.Log.Error(err, "manager failed")
		os.Exit(1)
	}
}
