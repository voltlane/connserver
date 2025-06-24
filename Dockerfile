FROM rust:alpine3.22 AS builder

WORKDIR /app

RUN apk add musl-dev

ADD clientcom ./clientcom
ADD clientcom-c ./clientcom-c
ADD connserver ./connserver
ADD enc ./enc
ADD net ./net
ADD Cargo.toml .
ADD Cargo.lock .

RUN cargo install --path connserver

FROM alpine:3.22

WORKDIR /voltlane

COPY --from=builder /usr/local/cargo/bin/connserver /usr/local/bin

CMD ["connserver"]
