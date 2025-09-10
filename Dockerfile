# See https://www.lpalmieri.com/posts/fast-rust-docker-builds/#cargo-chef for explanation
FROM --platform=$BUILDPLATFORM lukemathwalker/cargo-chef:0.1.72-rust-1.89.0 AS chef
WORKDIR /app


FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json


FROM chef AS builder
RUN apt-get update && apt-get install protobuf-compiler -y

COPY --from=planner /app/recipe.json recipe.json
RUN --mount=type=ssh cargo chef cook --release --recipe-path recipe.json

COPY . .
RUN --mount=type=ssh cargo build --release


FROM chef AS observer
COPY --from=builder /app/target/release/observer /app/observer

ENTRYPOINT ["/app/observer"]
