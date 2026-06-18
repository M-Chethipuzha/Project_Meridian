FROM golang:1.25-alpine AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /app ./services/amplifier

FROM alpine:3.21
RUN addgroup -S meridian && adduser -S meridian -G meridian
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /app /app
USER meridian
EXPOSE 8084
ENTRYPOINT ["/app"]
