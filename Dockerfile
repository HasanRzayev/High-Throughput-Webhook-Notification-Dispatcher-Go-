# Build stage
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git ca-certificates

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY . .

# Compile binaries with optimizations
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /bin/server ./cmd/server && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /bin/mockreceiver ./cmd/mockreceiver && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /bin/demo ./cmd/demo

# Final runtime stage
FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata curl && \
    addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /home/appuser

COPY --from=builder /bin/server /bin/server
COPY --from=builder /bin/mockreceiver /bin/mockreceiver
COPY --from=builder /bin/demo /bin/demo

USER appuser

EXPOSE 50051 8080

ENTRYPOINT ["/bin/server"]
