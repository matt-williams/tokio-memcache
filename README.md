# appengine-rust

Google App Engine custom runtime for Rust

## Toolchain installation

    curl https://sh.rustup.rs -sSf | sh
    rustup target add x86_64-unknown-linux-musl
    apt-get install protobuf-compiler
    cargo install protobuf

## Build

    cargo build --target=x86_64-unknown-linux-musl --release
    strip target/x86_64-unknown-linux-musl/release/appengine-rust
    gcloud beta debug source gen-repo-info-file

# Deploy

    gcloud init
    gcloud app deploy
