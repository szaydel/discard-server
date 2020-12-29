FROM golang:1.15
LABEL maintainer="Sam Zaydel <szaydel@gmail.com>"

WORKDIR /build
COPY go.mod go.sum server.go /build/
RUN go build -o discard-server

ENV PORT=5002
ENV METRICS_PORT=9094
ENV PPROF_PORT=6060
ENV INTERVAL=10s

EXPOSE ${PORT}
EXPOSE ${METRICS_PORT}
EXPOSE ${PPROF_PORT}

ENTRYPOINT ["./discard-server"]
