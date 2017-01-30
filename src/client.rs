use futures::{Future, Then, future};
use futures::future::FutureResult;
use tokio_core::net::TcpStream;
use tokio_core::reactor::Handle;
use tokio_proto::TcpClient;
use tokio_proto::pipeline::ClientService;
use tokio_service::Service;
use std::io;
use std::net::SocketAddr;

use request::Request;
use response::Response;
use value::Value;
use proto::Proto;
use api::Api;

pub struct Client {
    inner: ClientService<TcpStream, Proto>,
}

impl Client {
    pub fn connect(addr: &SocketAddr, handle: &Handle) -> Box<Future<Item = Client, Error = io::Error>> {
        Box::new(
            TcpClient::new(Proto)
                .connect(addr, handle)
                .map(|client_service| {
                    Client{inner: client_service}
                }))
    }
}

impl Service for Client {
    type Request = Request;
    type Response = Response;
    type Error = io::Error;
    type Future = Box<Future<Item = Response, Error = io::Error>>;

    fn call(&self, req: Request) -> Self::Future {
        Box::new(self.inner.call(req))
    }
}

impl<T: Service<Request = Request, Response = Response, Error = io::Error>> Api<io::Error> for T
    where T::Future: Future<Item = Response, Error = io::Error> + Sized {
    type FutureUnit = Then<T::Future, FutureResult<(), io::Error>, fn(Result<Response, io::Error>) -> FutureResult<(), io::Error>>;
    type FutureValues = Then<T::Future, FutureResult<Vec<Value>, io::Error>, fn(Result<Response, io::Error>) -> FutureResult<Vec<Value>, io::Error>>;
    type FutureU64 = Then<T::Future, FutureResult<u64, io::Error>, fn(Result<Response, io::Error>) -> FutureResult<u64, io::Error>>;
    type FutureString = Then<T::Future, FutureResult<String, io::Error>, fn(Result<Response, io::Error>) -> FutureResult<String, io::Error>>;

    fn set(&self, key: String, value: Vec<u8>, flags: u16, expiry: u32) -> Self::FutureUnit {
        fn map_result(result: Result<Response, io::Error>) -> FutureResult<(), io::Error> {
            future::result(match result {
                Ok(Response::Stored) => Ok(()),
                _ => Err(io::Error::new(io::ErrorKind::Other, "eek"))
            })
        }
        self.call(Request::Set{key: key, value: value, flags: flags, expiry: expiry, noreply: false})
            .then(map_result)
    }

    fn add(&self, key: String, value: Vec<u8>, flags: u16, expiry: u32) -> Self::FutureUnit {
        fn map_result(result: Result<Response, io::Error>) -> FutureResult<(), io::Error> {
            future::result(match result {
                Ok(Response::Stored) => Ok(()),
                _ => Err(io::Error::new(io::ErrorKind::Other, "eek"))
            })
        }
        self.call(Request::Add{key: key, value: value, flags: flags, expiry: expiry, noreply: false})
            .then(map_result)
    }

    fn replace(&self, key: String, value: Vec<u8>, flags: u16, expiry: u32) -> Self::FutureUnit {
        fn map_result(result: Result<Response, io::Error>) -> FutureResult<(), io::Error> {
            future::result(match result {
                Ok(Response::Stored) => Ok(()),
                _ => Err(io::Error::new(io::ErrorKind::Other, "eek"))
            })
        }
        self.call(Request::Replace{key: key, value: value, flags: flags, expiry: expiry, noreply: false})
            .then(map_result)
    }

    fn append(&self, key: String, value: Vec<u8>) -> Self::FutureUnit {
        fn map_result(result: Result<Response, io::Error>) -> FutureResult<(), io::Error> {
            future::result(match result {
                Ok(Response::Stored) => Ok(()),
                _ => Err(io::Error::new(io::ErrorKind::Other, "eek"))
            })
        }
        self.call(Request::Append{key: key, value: value, noreply: false})
            .then(map_result)
    }

    fn prepend(&self, key: String, value: Vec<u8>) -> Self::FutureUnit {
        fn map_result(result: Result<Response, io::Error>) -> FutureResult<(), io::Error> {
            future::result(match result {
                Ok(Response::Stored) => Ok(()),
                _ => Err(io::Error::new(io::ErrorKind::Other, "eek"))
            })
        }
        self.call(Request::Prepend{key: key, value: value, noreply: false})
            .then(map_result)
    }

    fn cas(&self, key: String, value: Vec<u8>, flags: u16, expiry: u32, cas: u64) -> Self::FutureUnit {
        fn map_result(result: Result<Response, io::Error>) -> FutureResult<(), io::Error> {
            future::result(match result {
                Ok(Response::Stored) => Ok(()),
                _ => Err(io::Error::new(io::ErrorKind::Other, "eek"))
            })
        }
        self.call(Request::Cas{key: key, value: value, flags: flags, expiry: expiry, cas: cas, noreply: false})
            .then(map_result)
    }

    fn get(&self, keys: Vec<String>) -> Self::FutureValues {
        fn map_result(result: Result<Response, io::Error>) -> FutureResult<Vec<Value>, io::Error> {
            future::result(match result {
                Ok(Response::Values(values)) => Ok(values),
                _ => Err(io::Error::new(io::ErrorKind::Other, "eek"))
            })
        }
        self.call(Request::Get{keys: keys})
            .then(map_result)
    }

    fn gets(&self, keys: Vec<String>) -> Self::FutureValues {
        fn map_result(result: Result<Response, io::Error>) -> FutureResult<Vec<Value>, io::Error> {
            future::result(match result {
                Ok(Response::Values(values)) => Ok(values),
                _ => Err(io::Error::new(io::ErrorKind::Other, "eek"))
            })
        }
        self.call(Request::Gets{keys: keys})
            .then(map_result)
    }

    fn delete(&self, key: String) -> Self::FutureUnit {
        fn map_result(result: Result<Response, io::Error>) -> FutureResult<(), io::Error> {
            future::result(match result {
                Ok(Response::Deleted) => Ok(()),
                _ => Err(io::Error::new(io::ErrorKind::Other, "eek"))
            })
        }
        self.call(Request::Delete{key: key, noreply: false})
            .then(map_result)
    }

    fn incr(&self, key: String, value: u64) -> Self::FutureU64 {
        fn map_result(result: Result<Response, io::Error>) -> FutureResult<u64, io::Error> {
            future::result(match result {
                Ok(Response::UpdatedValue(value)) => Ok(value),
                _ => Err(io::Error::new(io::ErrorKind::Other, "eek"))
            })
        }
        self.call(Request::Incr{key: key, value: value, noreply: false})
            .then(map_result)
    }

    fn decr(&self, key: String, value: u64) -> Self::FutureU64 {
        fn map_result(result: Result<Response, io::Error>) -> FutureResult<u64, io::Error> {
            future::result(match result {
                Ok(Response::UpdatedValue(value)) => Ok(value),
                _ => Err(io::Error::new(io::ErrorKind::Other, "eek"))
            })
        }
        self.call(Request::Incr{key: key, value: value, noreply: false})
            .then(map_result)
    }

    fn touch(&self, key: String, expiry: u32) -> Self::FutureUnit {
        fn map_result(result: Result<Response, io::Error>) -> FutureResult<(), io::Error> {
            future::result(match result {
                Ok(Response::Touched) => Ok(()),
                _ => Err(io::Error::new(io::ErrorKind::Other, "eek"))
            })
        }
        self.call(Request::Touch{key: key, expiry: expiry, noreply: false})
            .then(map_result)
    }

    fn flush_all(&self, delay: u32) -> Self::FutureUnit {
        fn map_result(result: Result<Response, io::Error>) -> FutureResult<(), io::Error> {
            future::result(match result {
                Ok(Response::Ok) => Ok(()),
                _ => Err(io::Error::new(io::ErrorKind::Other, "eek"))
            })
        }
        self.call(Request::FlushAll{delay: Some(delay), noreply: false})
            .then(map_result)
    }

    fn version(&self) -> Self::FutureString {
        fn map_result(result: Result<Response, io::Error>) -> FutureResult<String, io::Error> {
            future::result(match result {
                Ok(Response::Version(version)) => Ok(version),
                _ => Err(io::Error::new(io::ErrorKind::Other, "eek"))
            })
        }
        self.call(Request::Version)
            .then(map_result)
    }
}
