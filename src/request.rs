use std::str;
use std::str::FromStr;
use nom::digit;

use ::parse_utils::is_key_char;

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
    named!(pub parse<&[u8], Request>,
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

    pub fn build(&self, buf: &mut Vec<u8>) {
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
