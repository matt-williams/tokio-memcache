#[macro_use]
extern crate futures;
extern crate tokio_core;
extern crate tokio_memcache;

use futures::Future;
use tokio_core::reactor::Core;
use tokio_memcache::{Client, Api, ApiHelper, Logger};

pub fn main() {
    let mut core = Core::new().unwrap();
    let addr = "127.0.0.1:11211".parse().unwrap();
    let handle = core.handle();

    core.run(
        Client::connect(&addr, &handle)
        .and_then(|client| {
            let client = Logger::new(client);
            client.version()
            .and_then(move |version| {
                println!("Version: {}", version);
                client.set(String::from("abcd"), b"blah".to_vec(), 0, 0)
                .and_then(move |_| {
                    client.get_one(String::from("abcd"))
                    .and_then(|value| {
                        println!("{:?}", value);
                        Ok(())
                        })
                    })
            })
        })).unwrap();
}
