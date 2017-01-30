use futures::{Future, Then, future};
use futures::future::FutureResult;
use tokio_service::Service;
use std::str;
use std::fmt::Debug;

pub struct Logger<T> {
    inner: T,
}

impl<T> Logger<T> {
    pub fn new(inner: T) -> Logger<T> {
        Logger{inner: inner}
    }
}

impl<T> Service for Logger<T>
    where T: Service,
          <T as Service>::Request: Debug,
          <T as Service>::Response: Debug,
          <T as Service>::Error: Debug
{
    type Request = T::Request;
    type Response = T::Response;
    type Error = T::Error;
    type Future = Then<T::Future, FutureResult<T::Response, T::Error>, fn(Result<T::Response, T::Error>) -> FutureResult<Self::Response, Self::Error>>;

    fn call(&self, request: Self::Request) -> Self::Future {
        println!(">> {:?}", request);
        fn log<R: Debug, E: Debug>(result: Result<R, E>) -> FutureResult<R, E> {
            match result {
                Ok(ref response) => println!("<< {:?}", response),
                Err(ref error) => println!("!! {:?}", error),
            }
            future::result(result)
        }
        self.inner.call(request)
            .then(log::<Self::Response, Self::Error> as fn(Result<Self::Response, Self::Error>) -> FutureResult<Self::Response, Self::Error>)
    }
}
