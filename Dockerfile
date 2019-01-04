FROM golang:1.11.4-stretch

LABEL maintainer="support@inwecrypto.com"

COPY . /go/src/github.com/zkmq/zkmq

RUN go install github.com/zkmq/zkmq/cmd/broker && rm -rf /go/src

VOLUME ["/etc/cjoy/zkmq","/var/zkmq"]

WORKDIR /etc/cjoy/zkmq

EXPOSE 2017

CMD ["/go/bin/broker","-config","/etc/cjoy/zkmq/broker.json"]