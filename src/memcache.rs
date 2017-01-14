extern crate futures;
extern crate tokio_core;
extern crate tokio_proto;
extern crate tokio_service;

use self::futures::{future, Future};
use self::tokio_core::io::{Io, Codec, EasyBuf, Framed};
use self::tokio_core::net::TcpStream;
use self::tokio_core::reactor::Handle;
use self::tokio_proto::{TcpClient, TcpServer};
use self::tokio_proto::pipeline::{ServerProto, ClientProto, ClientService};
use self::tokio_service::{Service, NewService};
use std::{io, str};
use std::net::SocketAddr;

pub struct Client {
    inner: ClientService<TcpStream, LineProto>,
}

impl Client {
    /// Establish a connection to a line-based server at the provided `addr`.
    pub fn connect(addr: &SocketAddr, handle: &Handle) -> Box<Future<Item = Client, Error = io::Error>> {
        let ret = TcpClient::new(LineProto)
            .connect(addr, handle)
            .map(|client_service| {
                Client { inner: client_service }
            });

        Box::new(ret)
    }

    /// Send a `ping` to the remote. The returned future resolves when the
    /// remote has responded with a pong.
    ///
    /// This function provides a bit of sugar on top of the the `Service` trait.
    pub fn ping(&self) -> Box<Future<Item = (), Error = io::Error>> {
        // The `call` response future includes the string, but since this is a
        // "ping" request, we don't really need to include the "pong" response
        // string.
        let resp = self.call("[ping]".to_string())
            .and_then(|resp| {
                if resp != "[pong]" {
                    Err(io::Error::new(io::ErrorKind::Other, "expected pong"))
                } else {
                    Ok(())
                }
            });

        // Box the response future because we are lazy and don't want to define
        // a new future type and `impl T` isn't stable yet...
        Box::new(resp)
    }
}

impl Service for Client {
    type Request = String;
    type Response = String;
    type Error = io::Error;
    // For simplicity, box the future.
    type Future = Box<Future<Item = String, Error = io::Error>>;

    fn call(&self, req: String) -> Self::Future {
        Box::new(self.inner.call(req))
    }
}

/// Our line-based codec
pub struct LineCodec;

/// Protocol definition
struct LineProto;

/// Implementation of the simple line-based protocol.
///
/// Frames consist of a UTF-8 encoded string, terminated by a '\n' character.
impl Codec for LineCodec {
    type In = String;
    type Out = String;

    fn decode(&mut self, buf: &mut EasyBuf) -> Result<Option<String>, io::Error> {
        // Check to see if the frame contains a new line
        if let Some(n) = buf.as_ref().iter().position(|b| *b == b'\n') {
            // remove the serialized frame from the buffer.
            let line = buf.drain_to(n);

            // Also remove the '\n'
            buf.drain_to(1);

            // Turn this data into a UTF string and return it in a Frame.
            return match str::from_utf8(&line.as_ref()) {
                Ok(s) => Ok(Some(s.to_string())),
                Err(_) => Err(io::Error::new(io::ErrorKind::Other, "invalid string")),
            }
        }

        Ok(None)
    }

    fn encode(&mut self, msg: String, buf: &mut Vec<u8>) -> io::Result<()> {
        buf.extend_from_slice(msg.as_bytes());
        buf.push(b'\n');

        Ok(())
    }
}

impl<T: Io + 'static> ClientProto<T> for LineProto {
    type Request = String;
    type Response = String;

    /// `Framed<T, LineCodec>` is the return value of `io.framed(LineCodec)`
    type Transport = Framed<T, LineCodec>;
    type BindTransport = Result<Self::Transport, io::Error>;

    fn bind_transport(&self, io: T) -> Self::BindTransport {
        Ok(io.framed(LineCodec))
    }
}






/*
impl<T: Io + 'static> ServerProto<T> for Memcache {
    type Request = Request;
    type Response = Response;
    type Transport = Framed<T, MemcacheCodec>;
    type BindTransport = io::Result<Framed<T, MemcacheCodec>>;

    fn bind_transport(&self, io: T) -> io::Result<Framed<T, MemcacheCodec>> {
        Ok(io.framed(MemcacheCodec))
    }
}

pub struct MemcacheCodec;

impl Codec for MemcacheCodec {
    type In = memcache::Request;
    type Out = memcache::Response;

    fn decode(&mut self, buf: &mut EasyBuf) -> io::Result<Option<Request>> {
        request::decode(buf)
    }

    fn encode(&mut self, msg: Response, buf: &mut Vec<u8>) -> io::Result<()> {
        response::encode(msg, buf);
        Ok(())
    }
}
*/


#[cfg(test)]
mod tests {
    use memcache::tokio_core::reactor::Core;
    use memcache::tokio_service::{Service, NewService};
    use memcache::futures::{future, Future};
    use memcache::futures::future::AndThen;

    #[test]
    pub fn it_works() {
    	let mut core = Core::new().unwrap();
    
    	// This brings up our server.
    	let addr = "127.0.0.1:12345".parse().unwrap();
    
    	let handle = core.handle();
    
	core.run(
		::memcache::Client::connect(&addr, &handle)
		 .and_then(|client| {
			println!("Hello");
    			client.call("Hello".to_string())
    			.and_then(move |response| {
    				println!("CLIENT: {:?}", response);
    				client.call("Goodbye".to_string())
    				})
    			.and_then(|response| {
    				println!("CLIENT: {:?}", response);
    				Ok(())
    				})
    		})).unwrap();
    }
}
