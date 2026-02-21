FROM rust:bookworm AS builder
WORKDIR /workspace

RUN apt-get update \
    && apt-get install -y --no-install-recommends clang libclang-dev protobuf-compiler libprotobuf-dev \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY bins ./bins

RUN cargo build --release -p vectordb --bin vectordb-deploy

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /workspace/target/release/vectordb-deploy /usr/local/bin/vectordb-deploy

ENTRYPOINT ["/usr/local/bin/vectordb-deploy"]
