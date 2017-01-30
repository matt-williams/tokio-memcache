use futures::{Future, future};
use tokio_proto::TcpServer;
use tokio_service::{Service, NewService};
use std::io;
use std::net::SocketAddr;
use std::error::Error;

pub use request::Request;
pub use response::Response;
pub use value::Value;
pub use proto::Proto;
pub use api::Api;

pub struct ApiService<T> {
    api: T,
}

impl<T> ApiService<T> {
    pub fn new(api: T) -> ApiService<T> {
        ApiService{api: api}
    }
}

impl<T> Service for ApiService<T>
    where T: Api<io::Error>, // TODO: Genericize this over non-io errors.
          T::FutureUnit: Send + 'static,
          T::FutureValues: Send + 'static,
          T::FutureU64: Send + 'static,
          T::FutureString: Send + 'static {
    type Request = Request;
    type Response = Response;
    type Error = io::Error;
    type Future = Box<Future<Item = Response, Error = io::Error>>;

    fn call(&self, req: Request) -> Self::Future {
        match req {
            Request::Set{key, value, flags, expiry, noreply: _} => {
                self.api.set(key, value, flags, expiry)
                    .then(|result| {
                        future::done(match result {
                            Ok(()) => Ok(Response::Stored),
                            Err(err) => Ok(Response::ServerError(String::from(err.description()))),
                        })
                    }).boxed()
            },
            Request::Add{key, value, flags, expiry, noreply: _} => {
                self.api.add(key, value, flags, expiry)
                    .then(|result| {
                        future::done(match result {
                            Ok(()) => Ok(Response::Stored),
                            Err(err) => Ok(Response::ServerError(String::from(err.description()))),
                        })
                    }).boxed()
            },
            Request::Replace{key, value, flags, expiry, noreply: _} => {
                self.api.replace(key, value, flags, expiry)
                    .then(|result| {
                        future::done(match result {
                            Ok(()) => Ok(Response::Stored),
                            Err(err) => Ok(Response::ServerError(String::from(err.description()))),
                        })
                    }).boxed()
            },
            Request::Append{key, value, noreply: _} => {
                self.api.append(key, value)
                    .then(|result| {
                        future::done(match result {
                            Ok(()) => Ok(Response::Stored),
                            Err(err) => Ok(Response::ServerError(String::from(err.description()))),
                        })
                    }).boxed()
            },
            Request::Prepend{key, value, noreply: _} => {
                self.api.prepend(key, value)
                    .then(|result| {
                        future::done(match result {
                            Ok(()) => Ok(Response::Stored),
                            Err(err) => Ok(Response::ServerError(String::from(err.description()))),
                        })
                    }).boxed()
            },
            Request::Cas{key, value, flags, expiry, cas, noreply: _} => {
                self.api.cas(key, value, flags, expiry, cas)
                    .then(|result| {
                        future::done(match result {
                            Ok(()) => Ok(Response::Stored),
                            Err(err) => Ok(Response::ServerError(String::from(err.description()))),
                        })
                    }).boxed()
            },
            Request::Get{keys} => {
                self.api.get(keys)
                    .then(|result| {
                        future::done(match result {
                            Ok(values) => Ok(Response::Values(values)),
                            Err(err) => Ok(Response::ServerError(String::from(err.description()))),
                        })
                    }).boxed()
            },
            Request::Gets{keys} => {
                self.api.gets(keys)
                    .then(|result| {
                        future::done(match result {
                            Ok(values) => Ok(Response::Values(values)),
                            Err(err) => Ok(Response::ServerError(String::from(err.description()))),
                        })
                    }).boxed()
            },
            Request::Delete{key, noreply: _} => {
                self.api.delete(key)
                    .then(|result| {
                        future::done(match result {
                            Ok(()) => Ok(Response::Deleted),
                            Err(err) => Ok(Response::ServerError(String::from(err.description()))),
                        })
                    }).boxed()
            },
            Request::Incr{key, value, noreply: _} => {
                self.api.incr(key, value)
                    .then(|result| {
                        future::done(match result {
                            Ok(value) => Ok(Response::UpdatedValue(value)),
                            Err(err) => Ok(Response::ServerError(String::from(err.description()))),
                        })
                    }).boxed()
            },
            Request::Decr{key, value, noreply: _} => {
                self.api.decr(key, value)
                    .then(|result| {
                        future::done(match result {
                            Ok(value) => Ok(Response::UpdatedValue(value)),
                            Err(err) => Ok(Response::ServerError(String::from(err.description()))),
                        })
                    }).boxed()
            },
            Request::Touch{key, expiry, noreply: _} => {
                self.api.touch(key, expiry)
                    .then(|result| {
                        future::done(match result {
                            Ok(()) => Ok(Response::Stored),
                            Err(err) => Ok(Response::ServerError(String::from(err.description()))),
                        })
                    }).boxed()
            },
            Request::FlushAll{delay, noreply: _} => {
                self.api.flush_all(delay.unwrap_or(0))
                    .then(|result| {
                        future::done(match result {
                            Ok(()) => Ok(Response::Ok),
                            Err(err) => Ok(Response::ServerError(String::from(err.description()))),
                        })
                    }).boxed()
            },
            Request::Version => {
                self.api.version()
                    .then(|result| {
                        future::done(match result {
                            Ok(version) => Ok(Response::Version(version)),
                            Err(err) => Ok(Response::ServerError(String::from(err.description()))),
                        })
                    }).boxed()
            },
        }
    }
}

pub fn serve<T>(addr: SocketAddr, new_service: T)
    where T: NewService<Request = Request, Response = Response, Error = io::Error> + Send + Sync + 'static,
{
    TcpServer::new(Proto, addr).serve(new_service);
}
