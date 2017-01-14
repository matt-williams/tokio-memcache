FROM scratch
EXPOSE 8080
CMD ["/app"]
COPY app/target/x86_64-unknown-linux-musl/release/app /
