# engine-trino's own image: the job runner (`trino-job-runner`) and the
# rules service (`trino-rules`), on a distroless, non-root base. The
# coordinator runs Trino's image, mirrored. Build context: the
# repository's root:
#   docker build -f modules/engine-trino/Dockerfile -t engine-trino .
FROM rust:1.90-bookworm AS builder
WORKDIR /app
COPY . .
RUN cargo build --release --locked -p engine-trino-service

FROM gcr.io/distroless/cc-debian12:nonroot AS runtime
COPY --from=builder /app/target/release/trino-job-runner /usr/local/bin/trino-job-runner
COPY --from=builder /app/target/release/trino-rules /usr/local/bin/trino-rules
USER nonroot
