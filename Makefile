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
.PHONY: build-mcp-duckdb-linux-amd64 build-mcp-duckdb-linux-arm64
.PHONY: build-mcp-duckdb-darwin-amd64 build-mcp-duckdb-darwin-arm64
.PHONY: build-mcp-duckdb-all verify-dagnats-cgo-free

# ---------- Development targets ----------

build: ## Build dev binaries to ./bin
	go build -trimpath -ldflags="$(LDFLAGS)" -o bin/dagnats ./cmd/dagnats
	go build -trimpath -ldflags="$(LDFLAGS)" -o bin/dagnats-api ./cmd/dagnats-api
	go build -trimpath -ldflags="$(LDFLAGS)" -o bin/dagnats-engine ./cmd/dagnats-engine

test: ## Run all tests with 600s timeout
	# -p 4 bounds package-level parallelism. The suite stands up many
	# embedded NATS servers (engine, trigger, server, sidecar, and the
	# e2e superclusters spin up several each). At the default GOMAXPROCS
	# parallelism a high-core machine over-subscribes them — NATS
	# connects time out (~2s), workers miss their 5s start window, and
	# synchronous workflows blow their wait ceiling, so unrelated tests
	# flake non-deterministically (and `make release` flaps). Capping at
	# 4 concurrent packages keeps the gate deterministic; low-core CI
	# runners were already effectively bounded, so this is a no-op there.
	go test ./... -p 4 -timeout 600s -count=1

lint: vet ## Run gofmt + vet + staticcheck (matches CI)
	@out=$$(gofmt -l .); \
		if [ -n "$$out" ]; then \
			echo "gofmt: the following files are not formatted:"; \
			echo "$$out"; \
			echo "run 'make fmt' to fix"; \
			exit 1; \
		fi
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

# gomarkdoc embeds "View Source" links whose ref it auto-detects from git state.
# On a named branch it emits blob/main links; on CI's detached-HEAD PR checkout
# it falls back to commit-SHA links — so bare output drifts every commit and the
# drift guard below could never pass. Pinning url + default-branch + path makes
# the override explicit, so output is byte-identical regardless of git state or
# platform: committed == whatever CI regenerates. Keep all three in sync if the
# repo moves.
GOMARKDOC := gomarkdoc --repository.url https://github.com/danmestas/dagnats \
  --repository.default-branch main --repository.path /

docs-gen-sdk:
	$(GOMARKDOC) --output docs/site/content/docs/reference/sdk/dag/api.md ./dag/
	$(GOMARKDOC) --output docs/site/content/docs/reference/sdk/worker/api.md ./worker/
	$(GOMARKDOC) --output docs/site/content/docs/reference/sdk/protocol/api.md ./protocol/
	$(GOMARKDOC) --output docs/site/content/docs/reference/sdk/observe/api.md ./observe/
	$(GOMARKDOC) --output docs/site/content/docs/reference/sdk/actor/api.md ./actor/
	$(GOMARKDOC) --output docs/site/content/docs/reference/sdk/server/api.md ./server/
	$(GOMARKDOC) --output docs/site/content/docs/reference/sdk/bridge/api.md ./bridge/
	$(GOMARKDOC) --output docs/site/content/docs/reference/sdk/httpclient/api.md ./sdk/httpclient/
	$(GOMARKDOC) --output docs/site/content/docs/reference/sdk/dagnatstest/api.md ./dagnatstest/

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

# Build dagnats-mcp-duckdb for linux/amd64 with CGO + the bundled
# libduckdb.a from marcboeker/go-duckdb. The static link tag is
# the default in go-duckdb v1.8.x, so the resulting binary needs
# only glibc + libstdc++ + libm + libdl on the target host — all
# standard on every Ubuntu/Debian/RHEL release.
#
# We build inside a golang:1.26-bookworm container (linux/amd64)
# so the toolchain is reproducible and host-independent. Hosts
# need only Docker. The cmd/dagnats-mcp-duckdb directory is its
# own Go module, so the build runs from inside that subdir.
# See #188.
MCPDUCKDB_BUILDER_IMG := golang:1.26-bookworm
MCPDUCKDB_PKGNAME := dagnats-mcp-duckdb-linux-amd64
build-mcp-duckdb-linux-amd64: ## Build dagnats-mcp-duckdb linux/amd64 tarball
	@mkdir -p "$(DIST)/$(MCPDUCKDB_PKGNAME)"
	docker run --rm --platform linux/amd64 \
	  -v "$(CURDIR):/src" -w /src/cmd/dagnats-mcp-duckdb \
	  -e CGO_ENABLED=1 -e GOOS=linux -e GOARCH=amd64 \
	  -e GOCACHE=/tmp/gocache -e GOMODCACHE=/tmp/gomod \
	  $(MCPDUCKDB_BUILDER_IMG) \
	  go build -trimpath -ldflags="-s -w" \
	    -o "/src/$(DIST)/$(MCPDUCKDB_PKGNAME)/dagnats-mcp-duckdb" .
	@cp LICENSE README.md "$(DIST)/$(MCPDUCKDB_PKGNAME)/" 2>/dev/null || true
	@(cd $(DIST) && tar czf "$(MCPDUCKDB_PKGNAME).tar.gz" "$(MCPDUCKDB_PKGNAME)")
	@rm -rf "$(DIST)/$(MCPDUCKDB_PKGNAME)"
	@cd $(DIST) && shasum -a 256 $(MCPDUCKDB_PKGNAME).tar.gz >> SHA256SUMS
	@cd $(DIST) && shasum -a 256 $(MCPDUCKDB_PKGNAME).tar.gz
	@echo "" && echo "Built:" && ls -lh $(DIST)/$(MCPDUCKDB_PKGNAME).tar.gz

# linux/arm64: same Docker-based pattern as linux/amd64, just with
# --platform linux/arm64. Docker Desktop / containerd transparently
# uses qemu-user for the cross-arch build; on a native arm64 host
# (e.g. an Apple Silicon dev box) the build runs natively. The
# resulting binary depends on glibc/libstdc++/libm/libdl/libgcc_s
# and runs on every Ubuntu/Debian/RHEL release for arm64.
# See #188.
MCPDUCKDB_LINUX_ARM64_PKGNAME := dagnats-mcp-duckdb-linux-arm64
build-mcp-duckdb-linux-arm64: ## Build dagnats-mcp-duckdb linux/arm64 tarball
	@mkdir -p "$(DIST)/$(MCPDUCKDB_LINUX_ARM64_PKGNAME)"
	docker run --rm --platform linux/arm64 \
	  -v "$(CURDIR):/src" -w /src/cmd/dagnats-mcp-duckdb \
	  -e CGO_ENABLED=1 -e GOOS=linux -e GOARCH=arm64 \
	  -e GOCACHE=/tmp/gocache -e GOMODCACHE=/tmp/gomod \
	  $(MCPDUCKDB_BUILDER_IMG) \
	  go build -trimpath -ldflags="-s -w" \
	    -o "/src/$(DIST)/$(MCPDUCKDB_LINUX_ARM64_PKGNAME)/dagnats-mcp-duckdb" .
	@cp LICENSE README.md "$(DIST)/$(MCPDUCKDB_LINUX_ARM64_PKGNAME)/" 2>/dev/null || true
	@(cd $(DIST) && \
	  tar czf "$(MCPDUCKDB_LINUX_ARM64_PKGNAME).tar.gz" \
	    "$(MCPDUCKDB_LINUX_ARM64_PKGNAME)")
	@rm -rf "$(DIST)/$(MCPDUCKDB_LINUX_ARM64_PKGNAME)"
	@cd $(DIST) && \
	  shasum -a 256 $(MCPDUCKDB_LINUX_ARM64_PKGNAME).tar.gz >> SHA256SUMS
	@cd $(DIST) && shasum -a 256 $(MCPDUCKDB_LINUX_ARM64_PKGNAME).tar.gz
	@echo "" && echo "Built:" && \
	  ls -lh $(DIST)/$(MCPDUCKDB_LINUX_ARM64_PKGNAME).tar.gz

# darwin builds run natively on a macOS host (typically the GHA
# macos-latest runner, which is arm64). go-duckdb v1.8.x ships a
# static libduckdb.a for darwin/amd64 + darwin/arm64 under its
# deps/ tree, so the host needs only Xcode CommandLineTools (clang
# + the macOS SDK) — no DuckDB toolchain, no Homebrew install.
# CGO_ENABLED=1 plus -arch via CC selects the right slice. We pin
# MACOSX_DEPLOYMENT_TARGET=11.0 to match the dagnats main binary's
# minimum supported macOS. See #188.
MCPDUCKDB_DARWIN_DEPLOYMENT_TARGET := 11.0
MCPDUCKDB_DARWIN_AMD64_PKGNAME := dagnats-mcp-duckdb-darwin-amd64
build-mcp-duckdb-darwin-amd64: ## Build dagnats-mcp-duckdb darwin/amd64 tarball
	@if [ "$$(uname -s)" != "Darwin" ]; then \
	  echo "ERROR: build-mcp-duckdb-darwin-amd64 requires a macOS host"; \
	  exit 1; \
	fi
	@mkdir -p "$(DIST)/$(MCPDUCKDB_DARWIN_AMD64_PKGNAME)"
	cd cmd/dagnats-mcp-duckdb && \
	  CGO_ENABLED=1 GOOS=darwin GOARCH=amd64 \
	  CC="clang -arch x86_64" CXX="clang++ -arch x86_64" \
	  MACOSX_DEPLOYMENT_TARGET=$(MCPDUCKDB_DARWIN_DEPLOYMENT_TARGET) \
	  go build -trimpath -ldflags="-s -w" \
	    -o "$(CURDIR)/$(DIST)/$(MCPDUCKDB_DARWIN_AMD64_PKGNAME)/dagnats-mcp-duckdb" .
	@cp LICENSE README.md "$(DIST)/$(MCPDUCKDB_DARWIN_AMD64_PKGNAME)/" 2>/dev/null || true
	@(cd $(DIST) && \
	  tar czf "$(MCPDUCKDB_DARWIN_AMD64_PKGNAME).tar.gz" \
	    "$(MCPDUCKDB_DARWIN_AMD64_PKGNAME)")
	@rm -rf "$(DIST)/$(MCPDUCKDB_DARWIN_AMD64_PKGNAME)"
	@cd $(DIST) && \
	  shasum -a 256 $(MCPDUCKDB_DARWIN_AMD64_PKGNAME).tar.gz >> SHA256SUMS
	@cd $(DIST) && shasum -a 256 $(MCPDUCKDB_DARWIN_AMD64_PKGNAME).tar.gz
	@echo "" && echo "Built:" && \
	  ls -lh $(DIST)/$(MCPDUCKDB_DARWIN_AMD64_PKGNAME).tar.gz

MCPDUCKDB_DARWIN_ARM64_PKGNAME := dagnats-mcp-duckdb-darwin-arm64
build-mcp-duckdb-darwin-arm64: ## Build dagnats-mcp-duckdb darwin/arm64 tarball
	@if [ "$$(uname -s)" != "Darwin" ]; then \
	  echo "ERROR: build-mcp-duckdb-darwin-arm64 requires a macOS host"; \
	  exit 1; \
	fi
	@mkdir -p "$(DIST)/$(MCPDUCKDB_DARWIN_ARM64_PKGNAME)"
	cd cmd/dagnats-mcp-duckdb && \
	  CGO_ENABLED=1 GOOS=darwin GOARCH=arm64 \
	  CC="clang -arch arm64" CXX="clang++ -arch arm64" \
	  MACOSX_DEPLOYMENT_TARGET=$(MCPDUCKDB_DARWIN_DEPLOYMENT_TARGET) \
	  go build -trimpath -ldflags="-s -w" \
	    -o "$(CURDIR)/$(DIST)/$(MCPDUCKDB_DARWIN_ARM64_PKGNAME)/dagnats-mcp-duckdb" .
	@cp LICENSE README.md "$(DIST)/$(MCPDUCKDB_DARWIN_ARM64_PKGNAME)/" 2>/dev/null || true
	@(cd $(DIST) && \
	  tar czf "$(MCPDUCKDB_DARWIN_ARM64_PKGNAME).tar.gz" \
	    "$(MCPDUCKDB_DARWIN_ARM64_PKGNAME)")
	@rm -rf "$(DIST)/$(MCPDUCKDB_DARWIN_ARM64_PKGNAME)"
	@cd $(DIST) && \
	  shasum -a 256 $(MCPDUCKDB_DARWIN_ARM64_PKGNAME).tar.gz >> SHA256SUMS
	@cd $(DIST) && shasum -a 256 $(MCPDUCKDB_DARWIN_ARM64_PKGNAME).tar.gz
	@echo "" && echo "Built:" && \
	  ls -lh $(DIST)/$(MCPDUCKDB_DARWIN_ARM64_PKGNAME).tar.gz

# Convenience aggregate: build all four mcp-duckdb tarballs.
# Darwin targets require a macOS host; linux targets require
# Docker. The release pipeline invokes these per-platform on
# the matching GHA runner so this aggregate is for local dev
# only.
build-mcp-duckdb-all: build-mcp-duckdb-linux-amd64 \
                     build-mcp-duckdb-linux-arm64 \
                     build-mcp-duckdb-darwin-amd64 \
                     build-mcp-duckdb-darwin-arm64 ## Build all four mcp-duckdb tarballs
	@echo "" && echo "All mcp-duckdb artifacts:" && \
	  ls -lh $(DIST)/dagnats-mcp-duckdb-*.tar.gz

# Verify the main dagnats binary stays pure-Go (CGO-free) so the
# release tarball size and dependency surface for the dominant
# deployment path doesn't drift. Reads the build metadata embedded
# by `go build -trimpath` and fails if cgo was linked in. See #188.
verify-dagnats-cgo-free: ## Assert dist/dagnats-linux-amd64 is CGO-free
	@bin="$(DIST)/dagnats_$(VERSION)_linux_amd64/dagnats"; \
	if [ ! -f "$$bin" ]; then \
	  bin="$$(find $(DIST) -name 'dagnats' -path '*linux_amd64*' | head -1)"; \
	fi; \
	if [ -z "$$bin" ] || [ ! -f "$$bin" ]; then \
	  echo "ERROR: main dagnats linux/amd64 binary not found under $(DIST)"; \
	  exit 1; \
	fi; \
	if go version -m "$$bin" | grep -q '^\s*build\s*CGO_ENABLED=1'; then \
	  echo "ERROR: main dagnats must stay CGO-free, but $$bin has CGO_ENABLED=1"; \
	  exit 1; \
	fi; \
	echo "✓ $$bin is CGO-free"

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

release: release-preflight lint test build-release build-mcp-duckdb-linux-amd64 build-mcp-duckdb-linux-arm64 build-mcp-duckdb-darwin-amd64 build-mcp-duckdb-darwin-arm64 ## Build artifacts and publish a GitHub release for the current tag
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
