.PHONY: build test lint fmt vet serve clean
.PHONY: docs-serve docs-build docs-gen-sdk docs-gen-llms

build:
	go build -o bin/dagnats ./cmd/dagnats
	go build -o bin/dagnats-api ./cmd/dagnats-api
	go build -o bin/dagnats-engine ./cmd/dagnats-engine

test:
	go test ./... -timeout 120s -count=1

lint: vet
	@which staticcheck > /dev/null 2>&1 || \
		go install honnef.co/go/tools/cmd/staticcheck@latest
	staticcheck ./...

vet:
	go vet ./...

fmt:
	gofmt -w .

serve: build
	./bin/dagnats serve

clean:
	rm -rf bin/

# Documentation
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
