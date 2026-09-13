FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /bin/webhookd ./cmd/server && \
    CGO_ENABLED=0 go build -o /bin/webhook-receiver ./cmd/receiver

FROM alpine:3.20
RUN adduser -D -u 10001 app
USER app
COPY --from=build /bin/webhookd /bin/webhook-receiver /bin/
EXPOSE 8080
ENTRYPOINT ["/bin/webhookd"]
