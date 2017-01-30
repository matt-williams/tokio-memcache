use tokio_core::io::{Io, Codec, EasyBuf, Framed};
use tokio_proto::pipeline::{ServerProto, ClientProto};
use std::io;
use nom::IResult;

use request::Request;
use response::Response;

pub struct ClientCodec;

impl Codec for ClientCodec {
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
                (Err(io::Error::new(io::ErrorKind::Other, err)), 0)
            },
            IResult::Incomplete(_) => {
                (Ok(None), 0)
            }
        };
        buf.drain_to(bytes_used);
        result 
    }
}

pub struct ServerCodec;

impl Codec for ServerCodec {
    type In = Request;
    type Out = Response;

    fn decode(&mut self, buf: &mut EasyBuf) -> Result<Option<Request>, io::Error> {
        let buf_len = buf.len();
        let (result, bytes_used) = match { Request::parse(buf.as_slice()) } {
            IResult::Done(remaining, req) => {
                (Ok(Some(req)), buf_len - remaining.len())
            },
            IResult::Error(err) => {
                (Err(io::Error::new(io::ErrorKind::Other, err)), 0)
            },
            IResult::Incomplete(_) => {
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

pub struct Proto;

impl<T: Io + 'static> ClientProto<T> for Proto {
    type Request = Request;
    type Response = Response;
    type Transport = Framed<T, ClientCodec>;
    type BindTransport = Result<Self::Transport, io::Error>;

    fn bind_transport(&self, io: T) -> Self::BindTransport {
        Ok(io.framed(ClientCodec))
    }
}

impl<T: Io + 'static> ServerProto<T> for Proto {
    type Request = Request;
    type Response = Response;
    type Transport = Framed<T, ServerCodec>;
    type BindTransport = io::Result<Framed<T, ServerCodec>>;

    fn bind_transport(&self, io: T) -> Self::BindTransport {
        Ok(io.framed(ServerCodec))
    }
}

