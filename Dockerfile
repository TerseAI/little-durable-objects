FROM rust:1.89.0-bookworm AS builder

WORKDIR /build
COPY Cargo.toml Cargo.lock build.rs ./
COPY .cargo ./.cargo
COPY migrations ./migrations
COPY proto ./proto
COPY src ./src
RUN cargo build --locked --release

FROM debian:bookworm-slim

RUN apt-get update -qq \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/little-durable-objects /usr/local/bin/little-durable-objects

ENV RUST_LOG=warn,little_durable_objects=info
ENTRYPOINT ["/usr/local/bin/little-durable-objects"]
