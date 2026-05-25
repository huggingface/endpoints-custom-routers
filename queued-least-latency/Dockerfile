FROM golang:1.22-alpine AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o kvrouter .

FROM alpine:3.20
LABEL maintainer="Hugging Face"
RUN apk add --no-cache ca-certificates \
    && addgroup -g 1001 huggingface \
    && adduser -u 1001 -G huggingface -s /bin/sh -D huggingface
USER huggingface
COPY --from=builder /build/kvrouter /kvrouter
EXPOSE 3000
ENTRYPOINT ["/kvrouter"]
