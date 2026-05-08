# Meridian Stream — Consumer
# Build stage
FROM golang:1.25-alpine AS build
RUN apk add --no-cache gcc musl-dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /consumer ./services/consumer

# Runtime stage
FROM alpine:3.21
RUN apk add --no-cache ca-certificates tzdata
RUN adduser -D -u 1001 meridian
USER meridian
COPY --from=build /consumer /consumer
EXPOSE 8082
ENTRYPOINT ["/consumer"]
