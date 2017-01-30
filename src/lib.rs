#[macro_use]
extern crate nom;
extern crate futures;
extern crate tokio_core;
extern crate tokio_proto;
extern crate tokio_service;

use futures::{Future, Then, Map, future};
use futures::future::FutureResult;
use tokio_core::io::{Io, Codec, EasyBuf, Framed};
use tokio_core::net::TcpStream;
use tokio_core::reactor::Handle;
use tokio_proto::{TcpClient, TcpServer};
use tokio_proto::pipeline::{ServerProto, ClientProto, ClientService};
use tokio_service::{Service, NewService};
use std::{io, str};
use std::net::SocketAddr;
use std::error::Error;
use nom::IResult;

mod parse_utils;
mod request;
mod response;
mod value;
mod logger;

pub use request::Request;
pub use response::Response;
pub use value::Value;
pub use logger::Logger;

pub struct MemcacheClientCodec;

impl Codec for MemcacheClientCodec {
    type Out = Request;
    type In = Response;

    fn encode(&mut self, req: Request, buf: &mut Vec<u8>) -> io::Result<()> {
        req.build(buf);
        Ok(())
    }

    fn decode(&mut self, buf: &mut EasyBuf) -> Result<Option<Response>, io::Error> {
        let buf_len = buf.len();
        let (result, bytes_used) = match { Response::parse(buf.as_slice()) } {
            IResult::Done(remaining, rsp) => {
                (Ok(Some(rsp)), buf_len - remaining.len())
            },
            IResult::Error(err) => {
println!("Error: {:?}", buf.as_slice());
                (Err(io::Error::new(io::ErrorKind::Other, err)), 0)
            },
            IResult::Incomplete(_) => {
if buf_len > 0 {
    println!("Incomplete: {:?}", buf.as_slice());
}
                (Ok(None), 0)
            }
        };
        buf.drain_to(bytes_used);
        result 
    }
}

pub struct MemcacheServerCodec;

impl Codec for MemcacheServerCodec {
    type In = Request;
    type Out = Response;

    fn decode(&mut self, buf: &mut EasyBuf) -> Result<Option<Request>, io::Error> {
        let buf_len = buf.len();
        let (result, bytes_used) = match { Request::parse(buf.as_slice()) } {
            IResult::Done(remaining, req) => {
                (Ok(Some(req)), buf_len - remaining.len())
            },
            IResult::Error(err) => {
println!("Error: {:?}", buf.as_slice());
                (Err(io::Error::new(io::ErrorKind::Other, err)), 0)
            },
            IResult::Incomplete(_) => {
if buf_len > 0 {
    println!("Incomplete: {:?}", buf.as_slice());
}
                (Ok(None), 0)
            }
        };
        buf.drain_to(bytes_used);
        result 
    }

    fn encode(&mut self, rsp: Response, buf: &mut Vec<u8>) -> io::Result<()> {
        rsp.build(buf);
        Ok(())
    }
}

struct MemcacheProto;

impl<T: Io + 'static> ClientProto<T> for MemcacheProto {
    type Request = Request;
    type Response = Response;
    type Transport = Framed<T, MemcacheClientCodec>;
    type BindTransport = Result<Self::Transport, io::Error>;

    fn bind_transport(&self, io: T) -> Self::BindTransport {
        Ok(io.framed(MemcacheClientCodec))
    }
}

impl<T: Io + 'static> ServerProto<T> for MemcacheProto {
    type Request = Request;
    type Response = Response;
    type Transport = Framed<T, MemcacheServerCodec>;
    type BindTransport = io::Result<Framed<T, MemcacheServerCodec>>;

    fn bind_transport(&self, io: T) -> Self::BindTransport {
        Ok(io.framed(MemcacheServerCodec))
    }
}

pub struct Client {
    inner: ClientService<TcpStream, MemcacheProto>,
}

impl Client {
    pub fn connect(addr: &SocketAddr, handle: &Handle) -> Box<Future<Item = Client, Error = io::Error>> {
        Box::new(
            TcpClient::new(MemcacheProto)
                .connect(addr, handle)
                .map(|client_service| {
                    Client{inner: client_service}
                }))
    }
}

pub trait Api<E> {
    type FutureUnit: Future<Item = (), Error = E> + Sized;
    type FutureValues: Future<Item = Vec<Value>, Error = E> + Sized;
    type FutureU64: Future<Item = u64, Error = E> + Sized;
    type FutureString: Future<Item = String, Error = E> + Sized;

    fn set(&self, key: String, value: Vec<u8>, flags: u16, expiry: u32) -> Self::FutureUnit;
    fn add(&self, key: String, value: Vec<u8>, flags: u16, expiry: u32) -> Self::FutureUnit;
    fn replace(&self, key: String, value: Vec<u8>, flags: u16, expiry: u32) -> Self::FutureUnit;
    fn append(&self, key: String, value: Vec<u8>) -> Self::FutureUnit;
    fn prepend(&self, key: String, value: Vec<u8>) -> Self::FutureUnit;
    fn cas(&self, key: String, value: Vec<u8>, flags: u16, expiry: u32, cas: u64) -> Self::FutureUnit;
    fn get(&self, keys: Vec<String>) -> Self::FutureValues;
    fn gets(&self, keys: Vec<String>) -> Self::FutureValues;
    fn delete(&self, key: String) -> Self::FutureUnit;
    fn incr(&self, key: String, value: u64) -> Self::FutureU64;
    fn decr(&self, key: String, value: u64) -> Self::FutureU64;
    fn touch(&self, key: String, expiry: u32) -> Self::FutureUnit;
    fn flush_all(&self, delay: u32) -> Self::FutureUnit;
    fn version(&self) -> Self::FutureString;
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

pub trait ApiHelper<E> {
    type FutureValue: Future<Item = Value, Error = E> + Sized;

    fn get_one(&self, key: String) -> Self::FutureValue;
    fn gets_one(&self, key: String) -> Self::FutureValue;
}

impl<T, E> ApiHelper<E> for T
    where T: Api<E> {
    type FutureValue = Map<T::FutureValues, fn(Vec<Value>) -> Value>;

    fn get_one(&self, key: String) -> Self::FutureValue {
        fn map(values: Vec<Value>) -> Value {
            values[0].clone()
        }
        self.get(vec![key]).map(map)
    }

    fn gets_one(&self, key: String) -> Self::FutureValue {
        fn map(values: Vec<Value>) -> Value {
            values[0].clone()
        }
        self.gets(vec![key]).map(map)
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
    TcpServer::new(MemcacheProto, addr).serve(new_service);
}

#[cfg(test)]
mod tests {
    use ::tokio_core::reactor::Core;
    use ::futures::{Future, future};
    use ::futures::future::FutureResult;
    use ::{Value, Logger, Api, ApiHelper, ApiService};
    use std::thread;
    use std::time::Duration;

    pub struct ApiImpl;
    
    #[allow(unused_variables)]
    impl Api<::std::io::Error> for ApiImpl {
        type FutureUnit = FutureResult<(), ::std::io::Error>;
        type FutureValues = FutureResult<Vec<Value>, ::std::io::Error>;
        type FutureU64 = FutureResult<u64, ::std::io::Error>;
        type FutureString = FutureResult<String, ::std::io::Error>;

        fn set(&self, key: String, value: Vec<u8>, flags: u16, expiry: u32) -> Self::FutureUnit {
            future::done(Ok(()))
        }
        fn add(&self, key: String, value: Vec<u8>, flags: u16, expiry: u32) -> Self::FutureUnit {
            future::done(Ok(()))
        }
        fn replace(&self, key: String, value: Vec<u8>, flags: u16, expiry: u32) -> Self::FutureUnit {
            future::done(Ok(()))
        }
        fn append(&self, key: String, value: Vec<u8>) -> Self::FutureUnit {
            future::done(Ok(()))
        }
        fn prepend(&self, key: String, value: Vec<u8>) -> Self::FutureUnit {
            future::done(Ok(()))
        }
        fn cas(&self, key: String, value: Vec<u8>, flags: u16, expiry: u32, cas: u64) -> Self::FutureUnit {
            future::done(Ok(()))
        }
        fn get(&self, keys: Vec<String>) -> Self::FutureValues {
            future::done(Ok(keys.iter().map(|key| Value{key: key.clone(), value: (key.clone() + "'s value").as_bytes().to_vec(), flags: 0, cas: None}).collect()))
        }
        fn gets(&self, keys: Vec<String>) -> Self::FutureValues {
            future::done(Ok(keys.iter().map(|key| Value{key: key.clone(), value: (key.clone() + "'s value").as_bytes().to_vec(), flags: 0, cas: Some(8)}).collect()))
        }
        fn delete(&self, key: String) -> Self::FutureUnit {
            future::done(Ok(()))
        }
        fn incr(&self, key: String, value: u64) -> Self::FutureU64 {
            future::done(Ok(4))
        }
        fn decr(&self, key: String, value: u64) -> Self::FutureU64 {
            future::done(Ok(2))
        }
        fn touch(&self, key: String, expiry: u32) -> Self::FutureUnit {
            future::done(Ok(()))
        }
        fn flush_all(&self, delay: u32) -> Self::FutureUnit {
            future::done(Ok(()))
        }
        fn version(&self) -> Self::FutureString {
            future::done(Ok(String::from("1.2.3.4")))
        }
    }

    #[test]
    pub fn it_works() {
        let mut core = Core::new().unwrap();
    
        let addr = "127.0.0.1:11211".parse().unwrap();
    
        thread::spawn(move || {
            ::serve(addr, || {
                Ok(Logger::new(ApiService::new(ApiImpl{})))
            })
        });

        thread::sleep(Duration::from_millis(100));

        let handle = core.handle();
    
        core.run(
            ::Client::connect(&addr, &handle)
            .and_then(|client| {
                let client = Logger::new(client);
                client.version()
                .and_then(move |version| {
                    println!("Version: {}", version);
                    client.get_one(String::from("abcd"))
                    .and_then(move |value| {
                        println!("{:?}", value);
                        client.set(String::from("abcd"), b"blah".to_vec(), 0, 0)
                        .and_then(|_| {
                            Ok(())
                            })
                        })
                })
            })).unwrap();
    }
}
