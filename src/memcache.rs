extern crate futures;
extern crate tokio_core;
extern crate tokio_proto;
extern crate tokio_service;
extern crate service_fn;

use self::futures::{Future, Then, Map, future};
use self::futures::future::FutureResult;
use self::tokio_core::io::{Io, Codec, EasyBuf, Framed};
use self::tokio_core::net::TcpStream;
use self::tokio_core::reactor::Handle;
use self::tokio_proto::{TcpClient, TcpServer};
use self::tokio_proto::pipeline::{ServerProto, ClientProto, ClientService};
use self::tokio_service::{Service, NewService};
use std::{io, str};
use std::str::FromStr;
use std::net::SocketAddr;
use std::fmt::Debug;
use nom::{IResult, digit, not_line_ending};

#[inline]
fn is_key_char(chr: u8) -> bool {
  (chr >= 0x21 && chr <= 0x7e)
}

#[derive(Debug)]
pub enum Request {
    Set{key: String, value: Vec<u8>, flags: u16, expiry: u32, noreply: bool},
    Add{key: String, value: Vec<u8>, flags: u16, expiry: u32, noreply: bool},
    Replace{key: String, value: Vec<u8>, flags: u16, expiry: u32, noreply: bool},
    Append{key: String, value: Vec<u8>, noreply: bool},
    Prepend{key: String, value: Vec<u8>, noreply: bool},
    Cas{key: String, value: Vec<u8>, flags: u16, expiry: u32, cas: u64, noreply: bool},
    Get{keys: Vec<String>},
    Gets{keys: Vec<String>},
    Delete{key: String, noreply: bool},
    Incr{key: String, value: u64, noreply: bool},
    Decr{key: String, value: u64, noreply: bool},
    Touch{key: String, expiry: u32, noreply: bool},
    FlushAll{delay: Option<u32>, noreply: bool},
    Version,
}

impl Request {
    named!(parse<&[u8], Request>,
        alt!(
            chain!(
                tag!("set ") ~
                key: map_res!(take_while!(is_key_char), |x: &[u8]| String::from_utf8(x.to_vec())) ~
                tag!(" ") ~
                flags: map_res!(map_res!(digit, str::from_utf8), u16::from_str) ~
                tag!(" ") ~
                expiry: map_res!(map_res!(digit, str::from_utf8), u32::from_str) ~
                tag!(" ") ~
                len: map_res!(map_res!(digit, str::from_utf8), u32::from_str) ~
                noreply: map!(opt!(tag!(" noreply")), |x: Option<_>| x.is_some()) ~
                tag!("\r\n") ~
                value: map!(take!(len), |x: &[u8]| x.to_vec()) ~
                tag!("\r\n"),
                || Request::Set{key: key, value: value, flags: flags, expiry: expiry, noreply: noreply}) |
            chain!(
                tag!("add ") ~
                key: map_res!(take_while!(is_key_char), |x: &[u8]| String::from_utf8(x.to_vec())) ~
                tag!(" ") ~
                flags: map_res!(map_res!(digit, str::from_utf8), u16::from_str) ~
                tag!(" ") ~
                expiry: map_res!(map_res!(digit, str::from_utf8), u32::from_str) ~
                tag!(" ") ~
                len: map_res!(map_res!(digit, str::from_utf8), u32::from_str) ~
                noreply: map!(opt!(tag!(" noreply")), |x: Option<_>| x.is_some()) ~
                tag!("\r\n") ~
                value: map!(take!(len), |x: &[u8]| x.to_vec()) ~
                tag!("\r\n"),
                || Request::Add{key: key, value: value, flags: flags, expiry: expiry, noreply: noreply}) |
            chain!(
                tag!("replace ") ~
                key: map_res!(take_while!(is_key_char), |x: &[u8]| String::from_utf8(x.to_vec())) ~
                tag!(" ") ~
                flags: map_res!(map_res!(digit, str::from_utf8), u16::from_str) ~
                tag!(" ") ~
                expiry: map_res!(map_res!(digit, str::from_utf8), u32::from_str) ~
                tag!(" ") ~
                len: map_res!(map_res!(digit, str::from_utf8), u32::from_str) ~
                noreply: map!(opt!(tag!(" noreply")), |x: Option<_>| x.is_some()) ~
                tag!("\r\n") ~
                value: map!(take!(len), |x: &[u8]| x.to_vec()) ~
                tag!("\r\n"),
                || Request::Replace{key: key, value: value, flags: flags, expiry: expiry, noreply: noreply}) |
            chain!(
                tag!("append ") ~
                key: map_res!(take_while!(is_key_char), |x: &[u8]| String::from_utf8(x.to_vec())) ~
                tag!(" ") ~
                len: map_res!(map_res!(digit, str::from_utf8), u32::from_str) ~
                noreply: map!(opt!(tag!(" noreply")), |x: Option<_>| x.is_some()) ~
                tag!("\r\n") ~
                value: map!(take!(len), |x: &[u8]| x.to_vec()) ~
                tag!("\r\n"),
                || Request::Append{key: key, value: value, noreply: noreply}) |
            chain!(
                tag!("prepend ") ~
                key: map_res!(take_while!(is_key_char), |x: &[u8]| String::from_utf8(x.to_vec())) ~
                tag!(" ") ~
                len: map_res!(map_res!(digit, str::from_utf8), u32::from_str) ~
                noreply: map!(opt!(tag!(" noreply")), |x: Option<_>| x.is_some()) ~
                tag!("\r\n") ~
                value: map!(take!(len), |x: &[u8]| x.to_vec()) ~
                tag!("\r\n"),
                || Request::Prepend{key: key, value: value, noreply: noreply}) |
            chain!(
                tag!("cas ") ~
                key: map_res!(take_while!(is_key_char), |x: &[u8]| String::from_utf8(x.to_vec())) ~
                tag!(" ") ~
                flags: map_res!(map_res!(digit, str::from_utf8), u16::from_str) ~
                tag!(" ") ~
                expiry: map_res!(map_res!(digit, str::from_utf8), u32::from_str) ~
                tag!(" ") ~
                len: map_res!(map_res!(digit, str::from_utf8), u32::from_str) ~
                tag!(" ") ~
                cas: map_res!(map_res!(digit, str::from_utf8), u64::from_str) ~
                noreply: map!(opt!(tag!(" noreply")), |x: Option<_>| x.is_some()) ~
                tag!("\r\n") ~
                value: map!(take!(len), |x: &[u8]| x.to_vec()) ~
                tag!("\r\n"),
                || Request::Cas{key: key, value: value, flags: flags, expiry: expiry, cas: cas, noreply: noreply}) |
            chain!(
                tag!("get") ~
                keys: many0!(chain!(
                    tag!(" ") ~
                    key: map_res!(take_while!(is_key_char), |x: &[u8]| String::from_utf8(x.to_vec())),
                    || key)) ~
                tag!("\r\n"),
                || Request::Get{keys: keys}) |
            chain!(
                tag!("gets") ~
                keys: many0!(chain!(
                    tag!(" ") ~
                    key: map_res!(take_while!(is_key_char), |x: &[u8]| String::from_utf8(x.to_vec())),
                    || key)) ~
                tag!("\r\n"),
                || Request::Gets{keys: keys}) |
            chain!(
                tag!("delete ") ~
                key: map_res!(take_while!(is_key_char), |x: &[u8]| String::from_utf8(x.to_vec())) ~
                noreply: map!(opt!(tag!(" noreply")), |x: Option<_>| x.is_some()) ~
                tag!("\r\n"),
                || Request::Delete{key: key, noreply: noreply}) |
            chain!(
                tag!("incr ") ~
                key: map_res!(take_while!(is_key_char), |x: &[u8]| String::from_utf8(x.to_vec())) ~
                tag!(" ") ~
                value: map_res!(map_res!(digit, str::from_utf8), u64::from_str) ~
                noreply: map!(opt!(tag!(" noreply")), |x: Option<_>| x.is_some()) ~
                tag!("\r\n"),
                || Request::Incr{key: key, value: value, noreply: noreply}) |
            chain!(
                tag!("decr ") ~
                key: map_res!(take_while!(is_key_char), |x: &[u8]| String::from_utf8(x.to_vec())) ~
                tag!(" ") ~
                value: map_res!(map_res!(digit, str::from_utf8), u64::from_str) ~
                noreply: map!(opt!(tag!(" noreply")), |x: Option<_>| x.is_some()) ~
                tag!("\r\n"),
                || Request::Decr{key: key, value: value, noreply: noreply}) |
            chain!(
                tag!("touch ") ~
                key: map_res!(take_while!(is_key_char), |x: &[u8]| String::from_utf8(x.to_vec())) ~
                tag!(" ") ~
                expiry: map_res!(map_res!(digit, str::from_utf8), u32::from_str) ~
                noreply: map!(opt!(tag!(" noreply")), |x: Option<_>| x.is_some()) ~
                tag!("\r\n"),
                || Request::Touch{key: key, expiry: expiry, noreply: noreply}) |
            chain!(
                tag!("flush_all") ~
                delay: opt!(chain!(
                   tag!(" ") ~
                   delay: map_res!(map_res!(digit, str::from_utf8), u32::from_str),
                   || delay)) ~
                noreply: map!(opt!(tag!(" noreply")), |x: Option<_>| x.is_some()) ~
                tag!("\r\n"),
                || Request::FlushAll{delay: delay, noreply: noreply}) |
            map!(tag!("version\r\n"), |_| Request::Version)
        ));

    fn build(&self, buf: &mut Vec<u8>) {
        match *self {
            Request::Set{ref key, ref value, flags, expiry, noreply} => {
                buf.extend_from_slice(b"set ");
                buf.extend_from_slice(key.as_bytes());
                buf.extend_from_slice(b" ");
                buf.extend_from_slice(flags.to_string().as_bytes());
                buf.extend_from_slice(b" ");
                buf.extend_from_slice(expiry.to_string().as_bytes());
                buf.extend_from_slice(b" ");
                buf.extend_from_slice(value.len().to_string().as_bytes());
                if noreply {
                    buf.extend_from_slice(b" noreply");
                }
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(value.as_slice());
                buf.extend_from_slice(b"\r\n");
            },
            Request::Add{ref key, ref value, flags, expiry, noreply} => {
                buf.extend_from_slice(b"add ");
                buf.extend_from_slice(key.as_bytes());
                buf.extend_from_slice(b" ");
                buf.extend_from_slice(flags.to_string().as_bytes());
                buf.extend_from_slice(b" ");
                buf.extend_from_slice(expiry.to_string().as_bytes());
                buf.extend_from_slice(b" ");
                buf.extend_from_slice(value.len().to_string().as_bytes());
                if noreply {
                    buf.extend_from_slice(b" noreply");
                }
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(value.as_slice());
                buf.extend_from_slice(b"\r\n");
            },
            Request::Replace{ref key, ref value, flags, expiry, noreply} => {
                buf.extend_from_slice(b"replace ");
                buf.extend_from_slice(key.as_bytes());
                buf.extend_from_slice(b" ");
                buf.extend_from_slice(flags.to_string().as_bytes());
                buf.extend_from_slice(b" ");
                buf.extend_from_slice(expiry.to_string().as_bytes());
                buf.extend_from_slice(b" ");
                buf.extend_from_slice(value.len().to_string().as_bytes());
                if noreply {
                    buf.extend_from_slice(b" noreply");
                }
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(value.as_slice());
                buf.extend_from_slice(b"\r\n");
            },
            Request::Append{ref key, ref value, noreply} => {
                buf.extend_from_slice(b"append ");
                buf.extend_from_slice(key.as_bytes());
                buf.extend_from_slice(b" ");
                buf.extend_from_slice(value.len().to_string().as_bytes());
                if noreply {
                    buf.extend_from_slice(b" noreply");
                }
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(value.as_slice());
                buf.extend_from_slice(b"\r\n");
            },
            Request::Prepend{ref key, ref value, noreply} => {
                buf.extend_from_slice(b"prepend ");
                buf.extend_from_slice(key.as_bytes());
                buf.extend_from_slice(b" ");
                buf.extend_from_slice(value.len().to_string().as_bytes());
                if noreply {
                    buf.extend_from_slice(b" noreply");
                }
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(value.as_slice());
                buf.extend_from_slice(b"\r\n");
            },
            Request::Cas{ref key, ref value, flags, expiry, cas, noreply} => {
                buf.extend_from_slice(b"cas ");
                buf.extend_from_slice(key.as_bytes());
                buf.extend_from_slice(b" ");
                buf.extend_from_slice(flags.to_string().as_bytes());
                buf.extend_from_slice(b" ");
                buf.extend_from_slice(expiry.to_string().as_bytes());
                buf.extend_from_slice(b" ");
                buf.extend_from_slice(value.len().to_string().as_bytes());
                buf.extend_from_slice(b" ");
                buf.extend_from_slice(cas.to_string().as_bytes());
                if noreply {
                    buf.extend_from_slice(b" noreply");
                }
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(value.as_slice());
                buf.extend_from_slice(b"\r\n");
            },
            Request::Get{ref keys} => {
                buf.extend_from_slice(b"get");
                for key in keys.iter() {
                    buf.extend_from_slice(b" ");
                    buf.extend_from_slice(key.as_bytes());
                }
                buf.extend_from_slice(b"\r\n");
            },
            Request::Gets{ref keys} => {
                buf.extend_from_slice(b"gets");
                for key in keys.iter() {
                    buf.extend_from_slice(b" ");
                    buf.extend_from_slice(key.as_bytes());
                }
                buf.extend_from_slice(b"\r\n");
            },
            Request::Delete{ref key, noreply} => {
                buf.extend_from_slice(b"delete ");
                buf.extend_from_slice(key.as_bytes());
                if noreply {
                    buf.extend_from_slice(b" noreply");
                }
                buf.extend_from_slice(b"\r\n");
            },
            Request::Incr{ref key, value, noreply} => {
                buf.extend_from_slice(b"incr ");
                buf.extend_from_slice(key.as_bytes());
                buf.extend_from_slice(b" ");
                buf.extend_from_slice(value.to_string().as_bytes());
                if noreply {
                    buf.extend_from_slice(b" noreply");
                }
                buf.extend_from_slice(b"\r\n");
            },
            Request::Decr{ref key, value, noreply} => {
                buf.extend_from_slice(b"decr ");
                buf.extend_from_slice(key.as_bytes());
                buf.extend_from_slice(b" ");
                buf.extend_from_slice(value.to_string().as_bytes());
                if noreply {
                    buf.extend_from_slice(b" noreply");
                }
                buf.extend_from_slice(b"\r\n");
            },
            Request::Touch{ref key, expiry, noreply} => {
                buf.extend_from_slice(b"touch ");
                buf.extend_from_slice(key.as_bytes());
                buf.extend_from_slice(b" ");
                buf.extend_from_slice(expiry.to_string().as_bytes());
                if noreply {
                    buf.extend_from_slice(b" noreply");
                }
                buf.extend_from_slice(b"\r\n");
            },
            Request::FlushAll{delay, noreply} => {
                buf.extend_from_slice(b"flush_all ");
                match delay {
                    Some(delay) => {
                        buf.extend_from_slice(b" ");
                        buf.extend_from_slice(delay.to_string().as_bytes());
                    },
                    None => {}
                }
                if noreply {
                    buf.extend_from_slice(b" noreply");
                }
                buf.extend_from_slice(b"\r\n");
            },
            Request::Version => buf.extend_from_slice(b"version\r\n")
        }
    }
}

#[derive(Debug)]
pub enum Response {
    Error,
    ClientError(String),
    ServerError(String),
    Stored,
    NotStored,
    Exists,
    NotFound,
    Values(Vec<Value>),
    Deleted,
    UpdatedValue(u64),
    Touched,
    Ok,
    Version(String),
}

impl Response {
    named!(parse<&[u8], Response>,
        alt!(
            map!(tag!("ERROR\r\n"), |_| Response::Error) |
            chain!(
                tag!("CLIENT_ERROR ") ~
                message: map_res!(not_line_ending, |x: &[u8]| String::from_utf8(x.to_vec())) ~
                tag!("\r\n"),
                || Response::ClientError(message)) |
            chain!(
                tag!("SERVER_ERROR ") ~
                message: map_res!(not_line_ending, |x: &[u8]| String::from_utf8(x.to_vec())) ~
                tag!("\r\n"),
                || Response::ServerError(message)) |
            map!(tag!("STORED\r\n"), |_| Response::Stored) |
            map!(tag!("NOT STORED\r\n"), |_| Response::NotStored) |
            map!(tag!("EXISTS\r\n"), |_| Response::Exists) |
            map!(tag!("NOT FOUND\r\n"), |_| Response::NotFound) |
            chain!(
                values: many0!(Value::parse) ~
                tag!("END\r\n"),
                || Response::Values(values)) |
            map!(tag!("DELETED\r\n"), |_| Response::Deleted) |
            map!(tag!("TOUCHED\r\n"), |_| Response::Touched) |
            map!(tag!("OK\r\n"), |_| Response::Ok) |
            chain!(
                tag!("VERSION ") ~
                version: map_res!(not_line_ending, |x: &[u8]| String::from_utf8(x.to_vec())) ~
                tag!("\r\n"),
                || Response::Version(version))
        ));

    fn build(&self, buf: &mut Vec<u8>) {
        match *self {
            Response::Error => buf.extend_from_slice(b"ERROR\r\n"),
            Response::ClientError(ref message) => {
                buf.extend_from_slice(b"CLIENT_ERROR ");
                buf.extend_from_slice(message.as_bytes());
                buf.extend_from_slice(b"\r\n");
            },
            Response::ServerError(ref message) => {
                buf.extend_from_slice(b"SERVER_ERROR ");
                buf.extend_from_slice(message.as_bytes());
                buf.extend_from_slice(b"\r\n");
            },
            Response::Stored => buf.extend_from_slice(b"STORED\r\n"),
            Response::NotStored => buf.extend_from_slice(b"NOT STORED\r\n"),
            Response::Exists => buf.extend_from_slice(b"EXISTS\r\n"),
            Response::NotFound => buf.extend_from_slice(b"NOT FOUND\r\n"),
            Response::Values(ref values) => {
                for value in values.iter() {
                    value.build(buf);
                }
                buf.extend_from_slice(b"END\r\n");
            },
            Response::Deleted => buf.extend_from_slice(b"DELETED\r\n"),
            Response::UpdatedValue(value) => {
                buf.extend_from_slice(value.to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
            },
            Response::Touched => buf.extend_from_slice(b"TOUCHED\r\n"),
            Response::Ok => buf.extend_from_slice(b"OK\r\n"),
            Response::Version(ref version) => {
                buf.extend_from_slice(b"VERSION ");
                buf.extend_from_slice(version.as_bytes());
                buf.extend_from_slice(b"\r\n");
            },
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct Value {
    key: String,
    value: Vec<u8>,
    flags: u16,
    cas: Option<u64>,
}

impl Value {
    named!(parse<&[u8], Value>,
        chain!(
            tag!("VALUE ") ~
            key: map_res!(take_while!(is_key_char), |x: &[u8]| String::from_utf8(x.to_vec())) ~
            tag!(" ") ~
            flags: map_res!(map_res!(digit, str::from_utf8), u16::from_str) ~
            tag!(" ") ~
            len: map_res!(map_res!(digit, str::from_utf8), u32::from_str) ~
            cas: opt!(chain!(
                tag!(" ") ~
                cas: map_res!(map_res!(digit, str::from_utf8), u64::from_str),
                || cas)) ~
            tag!("\r\n") ~
            value: map!(take!(len), |x: &[u8]| x.to_vec()) ~
            tag!("\r\n"),
            || Value{key: key, value: value, flags: flags, cas: cas}));

    fn build(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(b"VALUE ");
        buf.extend_from_slice(self.key.as_bytes());
        buf.extend_from_slice(b" ");
        buf.extend_from_slice(self.flags.to_string().as_bytes());
        buf.extend_from_slice(b" ");
        buf.extend_from_slice(self.value.len().to_string().as_bytes());
        match self.cas {
            Some(cas) => {
                buf.extend_from_slice(b" ");
                buf.extend_from_slice(cas.to_string().as_bytes());
            },
            None => {}
        }
        buf.extend_from_slice(b"\r\n");
        buf.extend_from_slice(self.value.as_slice());
        buf.extend_from_slice(b"\r\n");
    }
}

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

pub trait Api {
    type FutureUnit: Future<Item = (), Error =io::Error> + Sized;
    type FutureValues: Future<Item = Vec<Value>, Error =io::Error> + Sized;
    type FutureU64: Future<Item = u64, Error =io::Error> + Sized;
    type FutureString: Future<Item = String, Error =io::Error> + Sized;

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

impl<T: Service<Request = Request, Response = Response, Error = io::Error>> Api for T
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

pub trait ApiHelper {
    type FutureValue: Future<Item = Value, Error =io::Error> + Sized;

    fn get_one(&self, key: String) -> Self::FutureValue;
    fn gets_one(&self, key: String) -> Self::FutureValue;
}

impl<T: Api> ApiHelper for T {
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

pub fn serve<T>(addr: SocketAddr, new_service: T)
    where T: NewService<Request = Request, Response = Response, Error = io::Error> + Send + Sync + 'static,
{
    TcpServer::new(MemcacheProto, addr).serve(new_service);
}

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

#[cfg(test)]
mod tests {
    use memcache::tokio_core::reactor::Core;
    use memcache::futures::Future;
    use memcache::{Request, Response, Value, Logger, Api, ApiHelper};
    use memcache::service_fn::service_fn;
    use std::thread;
    use std::time::Duration;

    #[test]
    pub fn it_works() {
        let mut core = Core::new().unwrap();
    
        let addr = "127.0.0.1:11211".parse().unwrap();
    
        thread::spawn(move || {
            // Use our `serve` fn
            ::memcache::serve(addr, || {
                Ok(Logger::new(service_fn(|req| {
                    match req {
                        Request::Get{keys} =>
                            Ok(Response::Values(keys.iter().map(|key| Value{key: key.clone(), value: (key.clone() + "'s value").as_bytes().to_vec(), flags: 0, cas: None}).collect())), // TODO Do I really need to clone key here?
                        Request::Gets{keys} =>
                            Ok(Response::Values(keys.iter().map(|key| Value{key: key.clone(), value: (key.clone() + "'s value").as_bytes().to_vec(), flags: 0, cas: Some(8)}).collect())), // TODO Do I really need to clone key here?
                        Request::Set{key: _, value: _, flags: _, expiry: _, noreply: _} =>
                            Ok(Response::Stored),
                        Request::Cas{key: _, value: _, flags: _, expiry: _, cas: _, noreply: _} =>
                            Ok(Response::Stored),
                        Request::Version =>
                            Ok(Response::Version(String::from("1.2.3.4"))),
                        _ => Ok(Response::Error),
                    }
                })))
            })
        });

        thread::sleep(Duration::from_millis(100));

        let handle = core.handle();
    
        core.run(
            ::memcache::Client::connect(&addr, &handle)
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
