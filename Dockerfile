# Stage 1: Build the binary
FROM golang:1.22-alpine AS builder

WORKDIR /app

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Build
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /app/opencode-collector ./cmd/opencode-collector

# Stage 2: Runtime image
FROM gcr.io/distroless/static:nonroot AS runtime

COPY --from=builder /app/opencode-collector /opencode-collector

ENTRYPOINT ["/opencode-collector"]
