# syntax=docker/dockerfile:1.4

# Build stage
FROM golang:1.26-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy go mod files first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the binary with parallel compilation and optimizations
# -p uses all available CPU cores for package compilation
# -ldflags="-s -w" strips debug symbols for faster linking
# -trimpath removes file system paths from binary
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 GOOS=linux \
    go build -p $(nproc) \
    -ldflags="-s -w" \
    -tags="no_duckdb_arrow" \
    -trimpath \
    -o /app/bin/server \
    ./main.go

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/bin/server /app/server

# Create data directory and symlink (for -trimpath compatibility)
RUN mkdir -p /app/data && ln -s /app/data /data

EXPOSE 8082

CMD ["/app/server", "-port", "8082"]
