ENVTEST_K8S_VERSION ?= 1.33
ENVTEST_TOOL_VERSION ?= v0.24.1
SETUP_ENVTEST := go run sigs.k8s.io/controller-runtime/tools/setup-envtest@$(ENVTEST_TOOL_VERSION)
IMAGE ?= ghcr.io/turingmotors/slurmkube
TAG ?= dev
CHART := deploy/chart/slurmkube
VERSION ?= 0.0.0-dev

.PHONY: build test test-race test-integration lint fmt vet image helm-lint helm-package manifests

build:
	go build ./...

fmt:
	gofmt -l -w cmd internal

vet:
	go vet -tags integration ./...

test:
	go test ./...

test-race:
	go test -race ./...

# envtest: a real kube-apiserver and etcd (downloaded once by setup-envtest).
test-integration:
	KUBEBUILDER_ASSETS="$$($(SETUP_ENVTEST) use $(ENVTEST_K8S_VERSION) -p path)" go test -p 1 -tags integration ./...

lint: fmt vet

image:
	docker build -t $(IMAGE):$(TAG) .

helm-lint:
	helm lint $(CHART)

helm-package:
	helm package $(CHART) --version $(VERSION) --app-version $(VERSION) -d dist

# Static manifests for kubectl/kustomize users: the chart's defaults, in the
# namespace and with the Secrets the README's install steps create.
manifests: helm-package
	helm template slurmkube dist/slurmkube-$(VERSION).tgz \
		--namespace slurm-jobs --set slurmTokenSecret=slurm-jwt > dist/install.yaml
