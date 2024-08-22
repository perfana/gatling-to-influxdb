FROM golang:1.20.6-alpine

WORKDIR /go/build

COPY . /go/build

RUN go build