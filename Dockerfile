FROM rust:latest AS builder

RUN apt-get update && \
    apt-get install -y \
    cmake \
    protobuf-compiler

WORKDIR /app

COPY . .

RUN cargo build --release
RUN strip target/release/etcd-reverse-proxy

# ======

FROM gcr.io/distroless/cc

WORKDIR /app

COPY --from=builder /app/target/release/etcd-reverse-proxy .

CMD ["/app/etcd-reverse-proxy", "-c", "config/init.lua"]
