ARG GOLANG_VERSION=1.23
FROM golang:${GOLANG_VERSION}-alpine AS build

ENV DISTRIBUTION_DIR /go/src/github.com/docker/distribution
ENV BUILDTAGS include_gcs

ARG GOOS=linux
ARG GOARCH=amd64

RUN apk --no-cache add make git file

WORKDIR $DISTRIBUTION_DIR
COPY go.mod go.sum ./
RUN go mod download
COPY . .

RUN CGO_ENABLED=0 make PREFIX=/go clean binaries \
    && file ./bin/registry | grep "statically linked"

FROM alpine:3.21

RUN apk --no-cache add ca-certificates curl

COPY --from=build /go/src/github.com/docker/distribution/bin/registry /bin/registry

COPY config/filesystem.yml /etc/docker/registry/config.yml

VOLUME ["/var/lib/registry"]
EXPOSE 5000

HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
  CMD curl --fail http://localhost:5000/v2/ || exit 1

CMD ["/bin/sh", "-c", "/bin/registry serve /etc/docker/registry/config.yml"]
