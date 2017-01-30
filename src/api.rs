use futures::{Future, Map};
use value::Value;

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
