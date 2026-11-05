FROM golang:1.27.3-bookworm@sha256:d9b6a1beb9474adc2a1ce842573639b44b2a0dd49b6669f51d3821baf9ec37c9 AS build
ENV CGO_ENABLED=1 GOTOOLCHAIN=local GOENV=off GOWORK=off GOFLAGS=-mod=readonly
WORKDIR /src
COPY go.mod go.sum ./
RUN sha256sum go.mod go.sum > /tmp/modules.sha256 && \
    test "$(go env GOVERSION)" = "go$(awk '$1 == "go" {print $2}' go.mod)" && \
    go mod download && go mod verify
COPY . .
ARG VERSION
ARG COMMIT=unknown
# Keep SQLite's CGO driver while avoiding shared libc/NSS and extension-loader dependencies.
RUN --mount=type=cache,target=/root/.cache/go-build \
    version="${VERSION:-v$(cat VERSION)-dev}" && \
    go build -trimpath -buildvcs=false -tags netgo,osusergo,sqlite_omit_load_extension \
    -ldflags "-s -w -buildid= -linkmode external -extldflags=-static -X github.com/livepeer/clearinghouse/internal/version.Version=${version} -X github.com/livepeer/clearinghouse/internal/version.Commit=${COMMIT}" \
    -o /out/clearinghouse ./cmd/clearinghouse && \
    sha256sum -c /tmp/modules.sha256

FROM scratch AS binaries
COPY --from=build /out/ /

FROM debian:bookworm-slim@sha256:99c0b25c46d0214d5a2091092e7908907dcaecd6a962cde0ac700acee222b6f3
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/*
COPY --from=binaries / /usr/local/bin/
ENTRYPOINT ["clearinghouse"]
