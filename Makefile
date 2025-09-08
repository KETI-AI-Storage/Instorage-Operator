# Image URL to use all building/pushing image targets
IMG ?= instorage-preprocess-operator:latest
# REGISTRY is the container registry to push to
REGISTRY ?= docker.io/your-org

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

# Setting SHELL to bash allows bash commands to be executed by recipes.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.PHONY: all
all: build

##@ General

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

.PHONY: test
test: fmt vet ## Run tests.
	go test ./... -coverprofile cover.out

##@ Build

.PHONY: build
build: fmt vet ## Build manager binary.
	go build -o bin/manager cmd/main.go

.PHONY: run
run: fmt vet ## Run a controller from your host.
	go run ./cmd/main.go

.PHONY: docker-build
docker-build: ## Build docker image with the manager.
	docker build -t ${IMG} .

.PHONY: docker-push
docker-push: ## Push docker image with the manager.
	docker push ${IMG}

.PHONY: docker-build-push
docker-build-push: docker-build docker-push ## Build and push docker image.

##@ Deployment

.PHONY: install
install: ## Install CRDs into the K8s cluster specified in ~/.kube/config.
	kubectl apply -f deploy/crd.yaml

.PHONY: uninstall
uninstall: ## Uninstall CRDs from the K8s cluster specified in ~/.kube/config.
	kubectl delete -f deploy/crd.yaml

.PHONY: deploy
deploy: ## Deploy controller to the K8s cluster specified in ~/.kube/config.
	kubectl apply -f deploy/operator.yaml

.PHONY: undeploy
undeploy: ## Undeploy controller from the K8s cluster specified in ~/.kube/config.
	kubectl delete -f deploy/operator.yaml

##@ Clean

.PHONY: clean
clean: ## Clean build artifacts
	rm -f bin/manager
	rm -f cover.out
	go clean -cache

##@ Release

.PHONY: release
release: docker-build docker-push ## Build and push release image
	@echo "Released ${IMG}"

# Example usage commands
.PHONY: example
example: ## Show example commands
	@echo "# Build and run locally:"
	@echo "make run"
	@echo ""
	@echo "# Build Docker image:"
	@echo "make docker-build IMG=myregistry/instorage-operator:v1.0.0"
	@echo ""
	@echo "# Deploy to cluster:"
	@echo "make install  # Install CRD"
	@echo "make deploy   # Deploy operator"
	@echo ""
	@echo "# Create sample job:"
	@echo "kubectl apply -f deploy/sample-job.yaml"