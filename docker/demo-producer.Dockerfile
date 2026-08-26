FROM golang:1.22-bookworm

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY ingestion ./ingestion
COPY cmd/demo-producer ./cmd/demo-producer
CMD ["go", "run", "./cmd/demo-producer"]
