FROM golang:1.18-alpine AS builder
RUN apk add --no-cache git
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o micros3 cmd/micros3/main.go

FROM alpine:3.16
RUN apk add --no-cache ca-certificates
WORKDIR /
COPY --from=builder /app/micros3 /micros3
EXPOSE 9000
EXPOSE 9001
ENTRYPOINT ["/micros3"]
