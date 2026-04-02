.PHONY: build test lint fmt vet serve clean

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
