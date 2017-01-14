extern crate futures;
extern crate hyper;

use hyper::{Get, Post, StatusCode};
use hyper::header::ContentLength;
use hyper::server::{Server, Service, Request, Response};

static INDEX: &'static [u8] = b"<!DOCTYPE html>\r\n<html><head><title>appengine-rust</title></head><body><h1>Google App Engine custom runtime for Rust</h1><p>Try POST /echo</p></body></html>";

#[derive(Clone, Copy)]
struct Echo;

impl Service for Echo {
    type Request = Request;
    type Response = Response;
    type Error = hyper::Error;
    type Future = ::futures::Finished<Response, hyper::Error>;

    fn call(&self, req: Request) -> Self::Future {
        ::futures::finished(match (req.method(), req.path()) {
            (&Get, Some("/")) | (&Get, Some("/echo")) => {
                Response::new()
                    .with_header(ContentLength(INDEX.len() as u64))
                    .with_body(INDEX)
            },
            (&Post, Some("/echo")) => {
                let mut res = Response::new();
                if let Some(len) = req.headers().get::<ContentLength>() {
                    res.headers_mut().set(len.clone());
                }
                res.with_body(req.body())
            },
            _ => {
                Response::new()
                    .with_status(StatusCode::NotFound)
            }
        })
    }
}


fn main() {
    let addr = "0.0.0.0:8080".parse().unwrap();
    let (listening, server) = Server::standalone(|tokio| {
        Server::http(&addr, tokio)?
            .handle(|| Ok(Echo), tokio)
    }).unwrap();
    println!("Listening on http://{}", listening);
    server.run();
}
