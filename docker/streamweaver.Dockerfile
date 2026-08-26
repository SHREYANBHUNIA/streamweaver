FROM golang:1.22-bookworm AS build

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential pkg-config librocksdb-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 go build -trimpath -ldflags="-s -w" -o /out/streamweaver ./cmd/streamweaver

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates librocksdb7.8 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=build /out/streamweaver /app/streamweaver

ENV STREAMWEAVER_HTTP_ADDR=:8080 \
    STREAMWEAVER_DATA_DIR=/var/lib/streamweaver/rocksdb \
    STREAMWEAVER_CHECKPOINT_DIR=/var/lib/streamweaver/checkpoints

VOLUME ["/var/lib/streamweaver"]
EXPOSE 8080
ENTRYPOINT ["/app/streamweaver"]
