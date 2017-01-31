#[macro_use]
extern crate futures;
extern crate tokio_memcache;

use futures::future;
use futures::future::FutureResult;
use tokio_memcache::{Api, ApiService, Value};

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

pub fn main() {
    let addr = "127.0.0.1:11211".parse().unwrap();

    tokio_memcache::serve(addr, || {
        Ok(ApiService::new(ApiImpl{}))
    });
}
