FROM docker.io/library/golang:1.25-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/cli       ./cmd/cli
RUN CGO_ENABLED=0 go build -o /out/store     ./cmd/store
RUN CGO_ENABLED=0 go build -o /out/notifier  ./cmd/notifier
RUN CGO_ENABLED=0 go build -o /out/scheduler ./cmd/scheduler
RUN CGO_ENABLED=0 go build -o /out/bot       ./cmd/bot

FROM docker.io/library/alpine:3.21
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /out/ /usr/local/bin/
