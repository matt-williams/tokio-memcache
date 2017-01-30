use nom::not_line_ending;

use ::value::Value;

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
    named!(pub parse<&[u8], Response>,
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

    pub fn build(&self, buf: &mut Vec<u8>) {
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
