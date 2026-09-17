export GOTOOLCHAIN := go1.27.1
export CGO_ENABLED := 1

.PHONY: build test vet check benchmark
build:
	go build -trimpath -o bin/clearinghouse ./cmd/clearinghouse

test:
	go test -race ./...

vet:
	go vet ./...

check: test vet build

benchmark:
	go test ./internal/app -bench BenchmarkSteadyState -benchmem
