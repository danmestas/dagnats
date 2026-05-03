# dagnats Makefile
# Build, test, lint, release, and Docker targets.

VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  := $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
DATE    := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

LDFLAGS := -s -w -X github.com/danmestas/dagnats/cli.Version=$(VERSION)

# Cross-platform release matrix. Each row is GOOS/GOARCH.
# Windows is excluded: the sidecar package uses Unix-only syscalls
# (Setpgid, Getpgid, Kill) for child-process supervision. A future
# build-tagged shim could enable Windows; tracked as a follow-up.
PLATFORMS := \
  linux/amd64 \
  linux/arm64 \
  darwin/amd64 \
  darwin/arm64

DIST := dist
BINS := dagnats dagnats-api dagnats-engine

# Docker image name. Override with: make docker DOCKER_IMAGE=ghcr.io/foo/bar
DOCKER_IMAGE ?= dagnats

.PHONY: help
help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

.PHONY: build test lint fmt vet serve clean
.PHONY: docs-serve docs-build docs-gen-sdk docs-gen-llms
.PHONY: build-release docker docker-push release release-preflight

# ---------- Development targets ----------

build: ## Build dev binaries to ./bin
	go build -trimpath -ldflags="$(LDFLAGS)" -o bin/dagnats ./cmd/dagnats
	go build -trimpath -ldflags="$(LDFLAGS)" -o bin/dagnats-api ./cmd/dagnats-api
	go build -trimpath -ldflags="$(LDFLAGS)" -o bin/dagnats-engine ./cmd/dagnats-engine

test: ## Run all tests with 600s timeout
	go test ./... -timeout 600s -count=1

lint: vet ## Run vet + staticcheck
	@which staticcheck > /dev/null 2>&1 || \
		go install honnef.co/go/tools/cmd/staticcheck@latest
	staticcheck ./...

vet: ## Run go vet
	go vet ./...

fmt: ## Format all Go files in place
	gofmt -w .

serve: build ## Build and run dagnats serve locally
	./bin/dagnats serve

clean: ## Remove build artifacts (bin/, dist/)
	rm -rf bin/ $(DIST)/

# ---------- Documentation targets ----------

docs-serve:
	hugo server -s docs/site

docs-build:
	hugo -s docs/site -d ../../dist

docs-gen-sdk:
	gomarkdoc --output docs/site/content/docs/reference/sdk/dag/api.md ./dag/
	gomarkdoc --output docs/site/content/docs/reference/sdk/worker/api.md ./worker/
	gomarkdoc --output docs/site/content/docs/reference/sdk/protocol/api.md ./protocol/
	gomarkdoc --output docs/site/content/docs/reference/sdk/observe/api.md ./observe/
	gomarkdoc --output docs/site/content/docs/reference/sdk/actor/api.md ./actor/
	gomarkdoc --output docs/site/content/docs/reference/sdk/server/api.md ./server/
	gomarkdoc --output docs/site/content/docs/reference/sdk/bridge/api.md ./bridge/
	gomarkdoc --output docs/site/content/docs/reference/sdk/httpclient/api.md ./sdk/httpclient/
	gomarkdoc --output docs/site/content/docs/reference/sdk/dagnatstest/api.md ./dagnatstest/

docs-gen-llms:  # Requires scripts/gen-llms-txt.sh (created later)
	./scripts/gen-llms-txt.sh

# ---------- Release targets ----------

build-release: clean ## Build cross-platform release binaries + tarballs into ./dist
	@mkdir -p $(DIST)
	@for platform in $(PLATFORMS); do \
	  os=$${platform%/*}; arch=$${platform#*/}; \
	  ext=""; [ "$${os}" = "windows" ] && ext=".exe"; \
	  pkgname="dagnats_$(VERSION)_$${os}_$${arch}"; \
	  pkgdir="$(DIST)/$${pkgname}"; \
	  mkdir -p "$${pkgdir}"; \
	  for bin in $(BINS); do \
	    out="$${pkgdir}/$${bin}$${ext}"; \
	    echo "Building $${out}"; \
	    CGO_ENABLED=0 GOOS=$${os} GOARCH=$${arch} go build \
	      -trimpath -ldflags="$(LDFLAGS)" -o "$${out}" ./cmd/$${bin} || exit 1; \
	  done; \
	  cp LICENSE README.md CHANGELOG.md "$${pkgdir}/" 2>/dev/null || true; \
	  if [ "$${os}" = "windows" ]; then \
	    (cd $(DIST) && zip -qr "$${pkgname}.zip" "$${pkgname}") && \
	    rm -rf "$${pkgdir}"; \
	  else \
	    (cd $(DIST) && tar czf "$${pkgname}.tar.gz" "$${pkgname}") && \
	    rm -rf "$${pkgdir}"; \
	  fi; \
	done
	@cd $(DIST) && for f in *.tar.gz *.zip; do \
	  if [ -f "$$f" ]; then shasum -a 256 "$$f"; fi; \
	done > SHA256SUMS
	@echo "" && echo "Built artifacts:" && ls -lh $(DIST)/

docker: ## Build Docker image (DOCKER_IMAGE=name to override)
	docker build \
	  --build-arg VERSION=$(VERSION) \
	  -t $(DOCKER_IMAGE):$(VERSION) \
	  -t $(DOCKER_IMAGE):latest \
	  .

docker-push: ## Push Docker image (requires login; set DOCKER_IMAGE to a registry path)
	@if [ "$(DOCKER_IMAGE)" = "dagnats" ]; then \
	  echo "ERROR: set DOCKER_IMAGE to a registry path, e.g. ghcr.io/danmestas/dagnats"; exit 1; \
	fi
	docker push $(DOCKER_IMAGE):$(VERSION)
	docker push $(DOCKER_IMAGE):latest

release-preflight:
	@if ! git diff --quiet || ! git diff --cached --quiet; then \
	  echo "ERROR: working directory has uncommitted changes"; exit 1; \
	fi
	@if ! git describe --tags --exact-match HEAD >/dev/null 2>&1; then \
	  echo "ERROR: HEAD is not at a tag. Tag first:"; \
	  echo "  git tag -a vX.Y.Z -m 'Release vX.Y.Z'"; \
	  echo "  git push origin vX.Y.Z"; \
	  exit 1; \
	fi

release: release-preflight lint test build-release ## Build artifacts and publish a GitHub release for the current tag
	@TAG=$$(git describe --tags --exact-match HEAD); \
	VERSION_NO_V=$${TAG#v}; \
	NOTES=$$(awk -v ver="$$VERSION_NO_V" '$$0 ~ "^## \\[" ver "\\]" {flag=1; next} flag && /^## \[/ {flag=0} flag' CHANGELOG.md); \
	if [ -z "$$NOTES" ]; then \
	  echo "ERROR: no [\$$VERSION_NO_V] section found in CHANGELOG.md"; exit 1; \
	fi; \
	echo "Creating GitHub release $$TAG"; \
	gh release create "$$TAG" \
	  --title "$$TAG" \
	  --notes "$$NOTES" \
	  $(DIST)/*.tar.gz $(DIST)/*.zip $(DIST)/SHA256SUMS
