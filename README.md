# appengine-rust

Google App Engine custom runtime for Rust

## Toolchain installation

    apt-get install protobuf-compiler
    cargo install protobuf
    curl https://sh.rustup.rs -sSf | sh
    rustup target add x86_64-unknown-linux-musl

## Build

    cargo build --target=x86_64-unknown-linux-musl --release
    strip app/target/x86_64-unknown-linux-musl/release/app

# Deploy

    gcloud init
    gcloud app deploy
