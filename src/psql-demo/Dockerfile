FROM rust:1.62 as builder
WORKDIR /usr/src/myapp
RUN apt-get update && apt-get install -y cmake
COPY . .
RUN cargo build --bin mz-psql-demo --release

FROM debian:buster-slim
RUN apt-get update && apt-get install -y extra-runtime-dependencies && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/src/myapp/target/release/mz-psql-demo /usr/local/bin/mz-psql-demo
CMD ["mz-psql-demo"]
