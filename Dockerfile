FROM rust:1.97-bookworm AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends clang cmake libclang-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/target/release/rustshell /usr/local/bin/rustshell

ENTRYPOINT ["rustshell"]
CMD ["--help"]
