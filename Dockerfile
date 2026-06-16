# Build stage
FROM golang:1.26-alpine AS builder

WORKDIR /app

# Copy dependency files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build statically linked binaries
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/kv-server ./server && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/kv-client ./client

# Final stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy binaries from builder
COPY --from=builder /app/kv-server .
COPY --from=builder /app/kv-client .

# Expose ports
EXPOSE 8001 8002 8003

# Run the server by default
ENTRYPOINT ["./kv-server"]
