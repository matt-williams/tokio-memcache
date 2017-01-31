use std::str;
use std::str::FromStr;
use nom::digit;
use std::io::Cursor;
use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};

use ::parse_utils::is_key_char;

const MAGIC_REQUEST: u8 = 0x80;

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
    pub fn parse(buf: &[u8]) -> Request {
        let mut rdr = Cursor::new(buf);
        let magic = rdr.read_u8
        
                || Request::Set{key: key, value: value, flags: flags, expiry: expiry, noreply: noreply}
                || Request::Add{key: key, value: value, flags: flags, expiry: expiry, noreply: noreply}
                || Request::Replace{key: key, value: value, flags: flags, expiry: expiry, noreply: noreply}
                || Request::Append{key: key, value: value, noreply: noreply}
                || Request::Prepend{key: key, value: value, noreply: noreply}
                || Request::Cas{key: key, value: value, flags: flags, expiry: expiry, cas: cas, noreply: noreply}
                || Request::Get{keys: keys}
                || Request::Gets{keys: keys}
                || Request::Delete{key: key, noreply: noreply}
                || Request::Incr{key: key, value: value, noreply: noreply}
                || Request::Decr{key: key, value: value, noreply: noreply}
                || Request::Touch{key: key, expiry: expiry, noreply: noreply}
                || Request::FlushAll{delay: delay, noreply: noreply}
                || Request::Version
    }

    pub fn build(&self, buf: &mut Vec<u8>) {
        buf.write_u8::<BigEndian>(MAGIC_REQUEST).unwrap();
        buf.write_u8::<BigEndian>(0x01).unwrap();
        buf.write_u16::<BigEndian>(key.len()).unwrap();
        buf.write_u8::<BigEndian>(0).unwrap();
        buf.write_u8::<BigEndian>(0).unwrap(); // Data type = raw bytes
        buf.write_u16::<BigEndian>(0).unwrap(); // vbucket id
        buf.write_u32::<BigEndian>(value.len()).unwrap();
        buf.write_u32::<BigEndian>(0).unwrap(); // opaque
        buf.write_u64::<BigEndian>(cas).unwrap(); // opaque

        match *self {
            Request::Set{ref key, ref value, flags, expiry, noreply} => {
                buf.push(MAGIC_BYTE_REQUEST);
                buf.push(0x01);
buf.extend_fro
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
