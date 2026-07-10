# syntax=docker/dockerfile:1.7
FROM rust:1.94-bookworm@sha256:6ae102bdbf528294bc79ad6e1fae682f6f7c2a6e6621506ba959f9685b308a55 AS builder

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    capnproto \
    clang \
    cmake \
    curl \
    git \
    libssl-dev \
    ninja-build \
    perl \
    pkg-config \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

COPY Cargo.toml Cargo.lock ./
COPY scripts ./scripts
COPY patches ./patches
RUN ./scripts/patch-crate.sh deno_crypto
COPY crates ./crates

RUN --mount=type=cache,id=dd-cargo-registry,target=/usr/local/cargo/registry \
  --mount=type=cache,id=dd-cargo-git,target=/usr/local/cargo/git \
  --mount=type=cache,id=dd-cargo-target,target=/workspace/target \
  cargo build --locked --profile dist \
  -p dd_server \
  -p dd_init \
  --no-default-features \
  --features dd_server/websocket,dd_server/otel \
  && mkdir -p /out \
  && cp /workspace/target/dist/dd_init /workspace/target/dist/dd_server /out/

FROM gcr.io/distroless/cc-debian12@sha256:a90cf0f046efb32466b38b0972fef3a95e7c580e392e79ff1b7ac08c15fed0bc AS runtime

WORKDIR /app
ENV BIND_PUBLIC_ADDR=0.0.0.0:8080
ENV BIND_PRIVATE_ADDR=[::]:8081
ENV PUBLIC_BASE_DOMAIN=example.com
ENV RUST_LOG=info

VOLUME ["/app/store"]

COPY --from=builder --chown=0:0 /out/dd_init /app/dd_init
COPY --from=builder --chown=0:0 /out/dd_server /app/dd_server

EXPOSE 8080
USER 0:0
ENTRYPOINT ["/app/dd_init"]
CMD ["/app/dd_server"]
