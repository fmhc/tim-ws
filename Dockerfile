FROM rust:1.90-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --locked --release --bins

FROM debian:bookworm-slim
COPY --from=build /src/target/release/tim-ws /usr/local/bin/tim-ws
COPY --from=build /src/target/release/tim-ws-healthcheck /usr/local/bin/tim-ws-healthcheck
USER 65532:65532
EXPOSE 9002
HEALTHCHECK --interval=15s --timeout=4s --start-period=15s --retries=3 CMD ["tim-ws-healthcheck"]
ENTRYPOINT ["tim-ws"]
CMD ["--help"]
