SHELL := /bin/bash
LOCALBIN := $(CURDIR)/bin
CONTROLLER_GEN := $(LOCALBIN)/controller-gen
ENVTEST := $(LOCALBIN)/setup-envtest
CONTROLLER_TOOLS_VERSION := v0.23.0
ENVTEST_VERSION := v0.25.2
ENVTEST_K8S_VERSION ?= 1.37.0
IMG ?= postgres-manager:dev
TEST_OUTPUT ?= $(LOCALBIN)/test-results
STATICCHECK_VERSION := v0.8.2
ACTIONLINT_VERSION := v1.7.12
GOVULNCHECK_VERSION := v1.9.0
STATICCHECK := $(LOCALBIN)/tools/staticcheck/$(STATICCHECK_VERSION)/staticcheck
ACTIONLINT := $(LOCALBIN)/tools/actionlint/$(ACTIONLINT_VERSION)/actionlint
GOVULNCHECK := $(LOCALBIN)/tools/govulncheck/$(GOVULNCHECK_VERSION)/govulncheck

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
	mkdir -p $(TEST_OUTPUT)
	go test -race -count=1 -covermode=atomic -coverprofile=$(TEST_OUTPUT)/unit.coverage.out ./...

test-integration: manifests $(ENVTEST)
	mkdir -p $(TEST_OUTPUT)
	KUBEBUILDER_ASSETS="$$($(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN)/envtest -p path)" go test -race -count=1 -tags=integration -covermode=atomic -coverpkg=./internal/controller,./internal/postgres,./internal/ha -coverprofile=$(TEST_OUTPUT)/integration.coverage.out -v ./internal/controller

.PHONY: verify verify-fmt verify-modules verify-generated verify-manifests lint lint-workflows security
verify: verify-fmt verify-modules verify-generated vet lint lint-workflows verify-manifests

# Verification fails on drift; unlike fmt, it never silently fixes Go source.
verify-fmt:
	@set -eu; files=$$(gofmt -l api cmd internal); if [ -n "$$files" ]; then printf 'Run make fmt for:\n%s\n' "$$files"; exit 1; fi

verify-modules:
	go mod tidy -diff
	go mod verify

verify-generated: manifests
	git diff --exit-code -- api/v1alpha1/zz_generated.deepcopy.go config/crd/bases config/rbac/role.yaml
	@set -eu; files=$$(git ls-files --others --exclude-standard -- api/v1alpha1/zz_generated.deepcopy.go config/crd/bases config/rbac/role.yaml); if [ -n "$$files" ]; then printf 'Commit generated files:\n%s\n' "$$files"; exit 1; fi

verify-manifests:
	@mkdir -p $(TEST_OUTPUT)
	kubectl kustomize config/default > $(TEST_OUTPUT)/install.yaml

lint: $(STATICCHECK)
	$(STATICCHECK) ./...
	$(STATICCHECK) -tags=integration ./internal/controller

lint-workflows: $(ACTIONLINT)
	$(ACTIONLINT)
	@set -e; for script in hack/*.sh; do bash -n "$$script"; done

security: $(GOVULNCHECK)
	$(GOVULNCHECK) ./...

$(STATICCHECK):
	mkdir -p $(@D)
	GOBIN=$(@D) go install honnef.co/go/tools/cmd/staticcheck@$(STATICCHECK_VERSION)

$(ACTIONLINT):
	mkdir -p $(@D)
	GOBIN=$(@D) go install github.com/rhysd/actionlint/cmd/actionlint@$(ACTIONLINT_VERSION)

$(GOVULNCHECK):
	mkdir -p $(@D)
	GOBIN=$(@D) go install golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION)

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
