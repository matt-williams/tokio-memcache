use std::str;
use std::str::FromStr;
use nom::digit;

use ::parse_utils::is_key_char;

#[derive(Debug, Default, Clone)]
pub struct Value {
    pub key: String,
    pub value: Vec<u8>,
    pub flags: u16,
    pub cas: Option<u64>,
}

impl Value {
    named!(pub parse<&[u8], Value>,
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

    pub fn build(&self, buf: &mut Vec<u8>) {
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

#[cfg(test)]
mod tests {
    use ::value::Value;

    #[test]
    fn build() {
        let mut buf = Vec::new();
        Value{key: String::from("key"), value: b"value".to_vec(), flags: 1, cas: Some(1)}.build(&mut buf);
        assert_eq!(b"VALUE key 1 5 1\r\nvalue\r\n", buf.as_slice());
    }
}
