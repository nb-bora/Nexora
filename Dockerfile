FROM rust:1-bookworm AS build
WORKDIR /w
COPY Cargo.toml Cargo.lock ./
COPY src src
RUN cargo build --release --locked && mkdir /data

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /w/target/release/nexora /nexora
# Non-root user must be able to write the ledger/tenants database.
COPY --from=build --chown=65532:65532 /data /data
ENV NEXORA_LEDGER_PATH=/data/nexora.db
VOLUME /data
EXPOSE 8080
ENTRYPOINT ["/nexora"]
