SHELL := /bin/bash
LOCALBIN := $(CURDIR)/bin
CONTROLLER_GEN := $(LOCALBIN)/controller-gen
ENVTEST := $(LOCALBIN)/setup-envtest
CONTROLLER_TOOLS_VERSION := v0.23.0
ENVTEST_VERSION := v0.25.2
ENVTEST_K8S_VERSION ?= 1.37.0
IMG ?= postgres-manager:dev

.PHONY: generate manifests fmt vet build test test-unit test-integration run install deploy docker-build test-e2e

generate: $(CONTROLLER_GEN)
	$(CONTROLLER_GEN) object paths="./api/..."

manifests: generate
	$(CONTROLLER_GEN) crd rbac:roleName=postgres-manager paths="./..." output:crd:artifacts:config=config/crd/bases output:rbac:artifacts:config=config/rbac

fmt:
	go fmt ./...

vet:
	go vet ./...

build: generate fmt vet
	go build -o bin/manager ./cmd/manager
	go build -o bin/postgres-manager-instance ./cmd/instance

test: manifests fmt vet test-unit test-integration

test-unit:
	go test -race -count=1 ./...

test-integration: manifests $(ENVTEST)
	KUBEBUILDER_ASSETS="$$($(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN)/envtest -p path)" go test -race -count=1 -tags=integration -v ./internal/controller

run: manifests
	go run ./cmd/manager --leader-elect=false

install: manifests
	kubectl apply -f config/crd/bases

deploy: manifests
	kubectl apply -k config/default

docker-build:
	docker build -t $(IMG) .

test-e2e: manifests
	bash hack/e2e.sh

.PHONY: test-e2e-ha postgres-image
test-e2e-ha: manifests
	bash hack/e2e-ha.sh

postgres-image:
	docker build -f Dockerfile.postgres -t postgres-manager-postgres:dev .

$(LOCALBIN):
	mkdir -p $(LOCALBIN)

$(CONTROLLER_GEN): | $(LOCALBIN)
	GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_TOOLS_VERSION)

$(ENVTEST): | $(LOCALBIN)
	GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-runtime/tools/setup-envtest@$(ENVTEST_VERSION)
