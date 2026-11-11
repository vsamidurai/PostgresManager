package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/vsamidurai/PostgresManager/internal/instance"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func main() {
	parameters := map[string]string{}
	allowed := map[string]bool{"max_connections": true, "shared_buffers": true, "work_mem": true, "maintenance_work_mem": true, "effective_cache_size": true, "log_min_duration_statement": true, "log_statement": true, "log_connections": true}
	flag.Func("parameter", "Initial PostgreSQL parameter", func(value string) error {
		k, v, ok := strings.Cut(value, "=")
		if !ok || !allowed[k] || strings.ContainsRune(v, 0) {
			return fmt.Errorf("unsupported PostgreSQL parameter")
		}
		parameters[k] = v
		return nil
	})
	flag.Parse()
	cfg := instance.ConfigFromEnvironment(parameters)
	if err := cfg.ValidateNames(); err != nil {
		slog.Error("Invalid configuration", "error", err)
		os.Exit(1)
	}
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		panic(err)
	}
	api, err := client.New(ctrl.GetConfigOrDie(), client.Options{Scheme: scheme})
	if err != nil {
		slog.Error("API client failed", "error", err)
		os.Exit(1)
	}
	manager, err := instance.New(cfg, api)
	if err != nil {
		slog.Error("Instance initialization held", "error", err)
		os.Exit(1)
	}
	if err := manager.Run(ctrl.SetupSignalHandler()); err != nil {
		slog.Error("Instance stopped without a completed fence", "error", err)
		os.Exit(1)
	}
}
