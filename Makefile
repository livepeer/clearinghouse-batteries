export GOTOOLCHAIN := go1.27.3
export CGO_ENABLED := 1

COMMIT ?= $(shell git rev-parse HEAD 2>/dev/null || echo unknown)
VERSION ?= v$(shell cat VERSION)-$(shell git rev-parse --short=12 HEAD 2>/dev/null || echo unknown)
DIRTY := $(if $(shell git status --porcelain --untracked-files=normal 2>/dev/null),-dirty)
LDFLAGS = -X github.com/livepeer/clearinghouse/internal/version.Version=$(VERSION)$(DIRTY) -X github.com/livepeer/clearinghouse/internal/version.Commit=$(COMMIT)

.PHONY: build test vet staticcheck vulncheck check benchmark
build:
	go build -trimpath -ldflags '$(LDFLAGS)' -o bin/clearinghouse ./cmd/clearinghouse

test:
	go test -race ./...

vet:
	go vet ./...

staticcheck:
	go run honnef.co/go/tools/cmd/staticcheck@v0.8.2 ./...

vulncheck:
	go run golang.org/x/vuln/cmd/govulncheck@v1.8.0 ./...

check: test vet staticcheck vulncheck build

benchmark:
	go test ./internal/app -bench BenchmarkSteadyState -benchmem
