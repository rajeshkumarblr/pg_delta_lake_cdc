# Build stage
FROM golang:1.26-alpine AS builder

WORKDIR /app

# Install dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY . .

# Build the application
RUN go build -o ingest ./cmd/ingest

# Final stage
FROM alpine:latest

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/ingest .

# Install ca-certificates for HTTPS requests
RUN apk add --no-cache ca-certificates

# Default command
CMD ["./ingest"]
