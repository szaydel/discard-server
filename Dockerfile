FROM golang:1.15
LABEL maintainer="Sam Zaydel <szaydel@gmail.com>"

WORKDIR /build
COPY go.mod go.sum server.go /build/
RUN go build -o discard-server

ENV INTERVAL=${INTERVAL:-10s}
ENV PORT=${PORT:-5002}
ENV METRICS_PORT=${METRICS_PORT:-9094}
ENV PPROF_PORT=${PPROF_PORT:-6060}

EXPOSE ${PORT}
EXPOSE ${METRICS_PORT}
EXPOSE ${PPROF_PORT}

ENTRYPOINT ["/bin/sh", "-c", "./discard-server -port ${PORT} -metrics.port ${METRICS_PORT} -pprof.port ${PPROF_PORT} -report.interval ${INTERVAL}"]
