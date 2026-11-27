# The orchestrator's image: the server (`orchestrator`), on a distroless,
# non-root base. Build context: the repository's root:
#   docker build -f modules/orchestrator/Dockerfile -t orchestrator .
FROM rust:1.90-bookworm AS builder
WORKDIR /app
COPY . .
RUN cargo build --release --locked -p orchestrator-server --bin orchestrator

FROM gcr.io/distroless/cc-debian12:nonroot AS runtime
COPY --from=builder /app/target/release/orchestrator /usr/local/bin/orchestrator
USER nonroot
ENTRYPOINT ["/usr/local/bin/orchestrator"]
