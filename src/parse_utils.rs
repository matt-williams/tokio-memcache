#[inline]
pub fn is_key_char(chr: u8) -> bool {
    (chr >= 0x21 && chr <= 0x7e)
}
