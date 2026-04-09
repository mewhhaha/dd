FROM rust:1-bookworm AS builder

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

RUN cargo build --locked --release -p dd_server

FROM gcr.io/distroless/cc-debian12 AS runtime

WORKDIR /app
ENV BIND_PUBLIC_ADDR=0.0.0.0:8080
ENV BIND_PRIVATE_ADDR=[::]:8081
ENV PUBLIC_BASE_DOMAIN=example.com
ENV RUST_LOG=info

VOLUME ["/app/store"]

COPY --from=builder /workspace/target/release/dd_server /app/dd_server

EXPOSE 8080
CMD ["/app/dd_server"]
