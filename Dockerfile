FROM scratch
EXPOSE 8080
CMD ["/appengine-rust"]
COPY target/x86_64-unknown-linux-musl/release/appengine-rust /
