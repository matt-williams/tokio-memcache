extern crate futures;
extern crate tokio_core;
extern crate tokio_proto;
extern crate tokio_service;
extern crate service_fn;

use self::futures::{Future, Then, future};
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

#[derive(Debug)]
pub enum Message {
    Req(Request),
    Rsp(Response)
}

impl Message {
    named!(parse<&[u8], Message>,
        alt!(
            map!(Request::parse, |x| Message::Req(x)) |
            map!(Response::parse, |x| Message::Rsp(x))
        ));

    fn build(&self, buf: &mut Vec<u8>) {
        match *self {
            Message::Req(ref x) => x.build(buf),
            Message::Rsp(ref x) => x.build(buf),
        }
    }
}

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
                || Request::Get{keys: keys}) |
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
            Request::Version => buf.extend_from_slice(b"VERSION\r\n")
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

pub struct MemcacheCodec;

impl Codec for MemcacheCodec {
    type In = Message;
    type Out = Message;

    fn decode(&mut self, buf: &mut EasyBuf) -> Result<Option<Message>, io::Error> {
        let buf_len = buf.len();
        let (result, bytes_used) = match { Message::parse(buf.as_slice()) } {
            IResult::Done(remaining, result) => {
                (Ok(Some(result)), buf_len - remaining.len())
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

    fn encode(&mut self, msg: Message, buf: &mut Vec<u8>) -> io::Result<()> {
        msg.build(buf);
        Ok(())
    }
}

struct MemcacheProto;

impl<T: Io + 'static> ClientProto<T> for MemcacheProto {
    type Request = Message;
    type Response = Message;
    type Transport = Framed<T, MemcacheCodec>;
    type BindTransport = Result<Self::Transport, io::Error>;

    fn bind_transport(&self, io: T) -> Self::BindTransport {
        Ok(io.framed(MemcacheCodec))
    }
}

impl<T: Io + 'static> ServerProto<T> for MemcacheProto {
    type Request = Message;
    type Response = Message;
    type Transport = Framed<T, MemcacheCodec>;
    type BindTransport = io::Result<Framed<T, MemcacheCodec>>;

    fn bind_transport(&self, io: T) -> Self::BindTransport {
        Ok(io.framed(MemcacheCodec))
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

pub trait MemcacheClient<F: Future<Item = Message, Error = io::Error> + Sized> {
    fn get(&self, key: String) -> Then<F, FutureResult<Value, io::Error>, fn(Result<Message, io::Error>) -> FutureResult<Value, io::Error>>;
    fn set(&self, key: String, value: Vec<u8>, flags: u16, expiry: u32) -> Then<F, FutureResult<(), io::Error>, fn(Result<Message, io::Error>) -> FutureResult<(), io::Error>>;
}

impl<F: Future<Item = Message, Error = io::Error> + Sized> MemcacheClient<F> for Service<Request = Message, Response = Message, Future = F, Error = io::Error> {
    fn get(&self, key: String) -> Then<F, FutureResult<Value, io::Error>, fn(Result<Message, io::Error>) -> FutureResult<Value, io::Error>> {
        fn map_result(result: Result<Message, io::Error>) -> FutureResult<Value, io::Error> {
            future::result(
                match result {
                    Ok(Message::Rsp(Response::Values(values))) => Ok(values[0].clone()),
                    _ => Err(io::Error::new(io::ErrorKind::Other, "eek"))
                }
            )
        }
        self.call(Message::Req(Request::Gets{keys: vec![key]}))
            .then(map_result)
    }

    fn set(&self, key: String, value: Vec<u8>, flags: u16, expiry: u32) -> Then<F, FutureResult<(), io::Error>, fn(Result<Message, io::Error>) -> FutureResult<(), io::Error>> {
        fn map_result(result: Result<Message, io::Error>) -> FutureResult<(), io::Error> {
            future::result(
                match result {
                    Ok(Message::Rsp(Response::Stored)) => Ok(()),
                    _ => Err(io::Error::new(io::ErrorKind::Other, "eek"))
                }
            )
        }
        self.call(Message::Req(Request::Set{key: key, value: value, flags: flags, expiry: expiry, noreply: false}))
            .then(map_result)
    }
}

impl Service for Client {
    type Request = Message;
    type Response = Message;
    type Error = io::Error;
    type Future = Box<Future<Item = Message, Error = io::Error>>;

    fn call(&self, req: Message) -> Self::Future {
        Box::new(self.inner.call(req))
    }
}

pub fn serve<T>(addr: SocketAddr, new_service: T)
    where T: NewService<Request = Message, Response = Message, Error = io::Error> + Send + Sync + 'static,
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
    use memcache::{Message, Request, Response, Value, Logger, MemcacheClient};
    use memcache::service_fn::service_fn;
    use std::thread;
    use std::time::Duration;
    use std::io;

    #[test]
    pub fn it_works() {
        let mut core = Core::new().unwrap();
    
        let addr = "127.0.0.1:11211".parse().unwrap();
    
        thread::spawn(move || {
            // Use our `serve` fn
            ::memcache::serve(addr, || {
                Ok(Logger::new(service_fn(|msg| {
                    match msg {
                        Message::Req(Request::Get{keys}) =>
                            Ok(Message::Rsp(Response::Values(keys.iter().map(|key| Value{key: key.clone(), value: (key.clone() + "'s value").as_bytes().to_vec(), flags: 0, cas: None}).collect()))), // TODO Do I really need to clone key here?
                        Message::Req(Request::Gets{keys}) =>
                            Ok(Message::Rsp(Response::Values(keys.iter().map(|key| Value{key: key.clone(), value: (key.clone() + "'s value").as_bytes().to_vec(), flags: 0, cas: None}).collect()))), // TODO Do I really need to clone key here?
                        Message::Req(Request::Set{key: _, value: _, flags: _, expiry: _, noreply: _}) =>
                            Ok(Message::Rsp(Response::Stored)),
                        Message::Req(Request::Cas{key: _, value: _, flags: _, expiry: _, cas: _, noreply: _}) =>
                            Ok(Message::Rsp(Response::Stored)),
                        _ => Ok(Message::Rsp(Response::Error)),
                    }
                })))
            })
        });

        thread::sleep(Duration::from_millis(100));

        let handle = core.handle();
    
        core.run(
            ::memcache::Client::connect(&addr, &handle)
            .and_then(|client| {
//               let client = Logger::new(client);
               (client as MemcacheClient<Box<Future<Item = Message, Error = io::Error>>>).get(String::from("abcd"))
               .and_then(move |value| {
                   println!("{:?}", value);
                   client.set(String::from("abcd"), b"blah".to_vec(), 0, 0)
                   })
               .and_then(|_| {
                   Ok(())
                   })
            })).unwrap();
    }
}
