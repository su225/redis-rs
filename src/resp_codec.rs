use std::io::{Cursor, Error};
use std::str::Utf8Error;
use num::Num;
use num_bigint::BigInt;
use tokio_util::bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

#[non_exhaustive]
pub struct RESPOpcode;

impl RESPOpcode {
    pub const SIMPLE_STRING: u8   = b'+';
    pub const SIMPLE_ERROR: u8    = b'-';
    pub const INTEGER: u8         = b':';
    pub const BULK_STRING: u8     = b'$';
    pub const ARRAY: u8           = b'*';
    pub const NULL: u8            = b'_';
    pub const BOOLEAN: u8         = b'#';
    pub const DOUBLE: u8          = b',';
    pub const BIG_NUMBER: u8      = b'(';
    pub const BULK_ERROR: u8      = b'!';
    pub const VERBATIM_STRING: u8 = b'=';
    pub const MAP: u8             = b'%';
    pub const SET: u8             = b'~';
    pub const PUSH: u8            = b'>';
}

pub struct RESPCodec;

#[derive(Debug, PartialEq)]
pub enum RESPItem {
    SimpleString(String),          // +
    SimpleError(String),           // -
    Integer(i64),                  // :
    BulkString(String),            // $
    Array(Vec<RESPItem>),          // *
    Null,                          // _
    Boolean(bool),                 // #
    Double(f64),                   // ,
    BigNumber(BigInt),             // (
    BulkError(String),             // !
    VerbatimString{
        encoding: String,
        data: String,
    },                              // =
    Map(Vec<(RESPItem, RESPItem)>), // %
    Set(Vec<RESPItem>),             // ~
    Push(Vec<RESPItem>),            // >
}

impl RESPItem {
    fn opcode(&self) -> u8 {
        match self {
            RESPItem::SimpleString(_) => RESPOpcode::SIMPLE_STRING,
            RESPItem::SimpleError(_) => RESPOpcode::SIMPLE_ERROR,
            RESPItem::Integer(_) => RESPOpcode::INTEGER,
            RESPItem::BulkString(_) => RESPOpcode::BULK_STRING,
            RESPItem::Array(_) => RESPOpcode::ARRAY,
            RESPItem::Null => RESPOpcode::NULL,
            RESPItem::Boolean(_) => RESPOpcode::BOOLEAN,
            RESPItem::Double(_) => RESPOpcode::DOUBLE,
            RESPItem::BigNumber(_) => RESPOpcode::BIG_NUMBER,
            RESPItem::BulkError(_) => RESPOpcode::BULK_ERROR,
            RESPItem::VerbatimString { .. } => RESPOpcode::VERBATIM_STRING,
            RESPItem::Map(_) => RESPOpcode::MAP,
            RESPItem::Set(_) => RESPOpcode::SET,
            RESPItem::Push(_) => RESPOpcode::PUSH,
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum RESPParseError {
    Inner(String),
    Incomplete,
    MalformedInteger,
    MalformedBulkStringLengthMismatch,
    MalformedArrayNegativeLength,
    UnknownCommandType(char),
    MalformedNull,
    MalformedBoolean(String),
    MalformedDouble,
    VerbatimStringMustAtLeastHave4Chars,
    VerbatimStringFormatMalformed,
    NegativeLength,
    MalformedUtf8String,
    MalformedCommand,
    AggregateError(usize, Box<RESPParseError>),
}

impl From<Error> for RESPParseError {
    fn from(value: Error) -> Self {
        RESPParseError::Inner(value.to_string())
    }
}

impl From<Utf8Error> for RESPParseError {
    fn from(_value: Utf8Error) -> Self {
        RESPParseError::MalformedUtf8String
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum RESPEncodeError {
    Inner(String),
    VerbatimStringEncodingNameLength,
}

impl From<Error> for RESPEncodeError {
    fn from(value: Error) -> Self {
        RESPEncodeError::Inner(value.to_string())
    }
}

impl RESPCodec {
    pub fn new() -> Self {
        RESPCodec {}
    }

    fn decode_redis_command(&mut self, buf: &mut BytesMut) -> Result<Option<RESPItem>, RESPParseError> {
        let mut cursor = Cursor::new(&buf[..]);
        match self.check_redis_command(&mut cursor) {
            Ok(()) => {
                cursor.set_position(0);
                self.parse_redis_command(&mut cursor)
            },
            Err(RESPParseError::Incomplete) => Ok(None),
            Err(err) => Err(err),
        }
    }

    fn check_redis_command(&mut self, buf: &mut Cursor<&[u8]>) -> Result<(), RESPParseError> {
        if !buf.has_remaining() {
            return Err(RESPParseError::Incomplete);
        }
        let opcode = buf.get_u8();
        match opcode {
            RESPOpcode::SIMPLE_STRING | RESPOpcode::SIMPLE_ERROR => {
                let _ = get_line(buf)?;
                Ok(())
            },
            RESPOpcode::INTEGER => {
                let _ = get_integer(buf)?;
                Ok(())
            },
            RESPOpcode::BULK_STRING | RESPOpcode::BULK_ERROR => {
                let str_len = get_integer(buf)?;
                if str_len < -1 {
                    Err(RESPParseError::NegativeLength)
                } else if str_len == -1 {
                    Ok(())
                } else {
                    let str = get_line(buf)?;
                    if str.len() != str_len as usize {
                        Err(RESPParseError::MalformedBulkStringLengthMismatch)
                    } else {
                        Ok(())
                    }
                }
            },
            RESPOpcode::ARRAY | RESPOpcode::SET | RESPOpcode::PUSH => {
                let arr_len = get_integer(buf)?;
                if arr_len < 0 {
                    Err(RESPParseError::MalformedArrayNegativeLength)
                } else {
                    let mut elem_cnt = 0;
                    while elem_cnt < arr_len {
                        self.check_redis_command(buf)?;
                        elem_cnt += 1;
                    }
                    return Ok(());
                }
            },
            RESPOpcode::NULL => {
                let s = get_line(buf)?;
                if !s.is_empty() {
                    Err(RESPParseError::MalformedNull)
                } else {
                    Ok(())
                }
            },
            RESPOpcode::BOOLEAN => {
                let s = get_line(buf)?;
                if s.len() != 1 {
                    let invalid_bool_val = std::str::from_utf8(s)?.to_string();
                    Err(RESPParseError::MalformedBoolean(invalid_bool_val))
                } else {
                    Ok(())
                }
            },
            RESPOpcode::DOUBLE => {
                let _s = get_line(buf)?;
                Ok(())
            },
            RESPOpcode::BIG_NUMBER => {
                let _s = get_line(buf)?;
                Ok(())
            },
            RESPOpcode::VERBATIM_STRING => {
                let str_len = get_integer(buf)?;
                if str_len < -1 {
                    Err(RESPParseError::NegativeLength)
                } else {
                    let str = get_line(buf)?;
                    if str.len() != str_len as usize {
                        Err(RESPParseError::MalformedBulkStringLengthMismatch)
                    } else if str.len() < 4 {
                        Err(RESPParseError::VerbatimStringMustAtLeastHave4Chars)
                    } else if str[3] != b':' {
                        Err(RESPParseError::VerbatimStringFormatMalformed)
                    } else {
                        Ok(())
                    }
                }
            },
            RESPOpcode::MAP => {
                let num_key_values = get_integer(buf)?;
                if num_key_values < 0 {
                    Err(RESPParseError::NegativeLength)
                } else {
                    let mut check_cnt = 0;
                    while check_cnt < num_key_values {
                        self.check_redis_command(buf)?; // key
                        self.check_redis_command(buf)?; // value
                        check_cnt += 1;
                    }
                    return Ok(());
                }
            },
            _ => Err(RESPParseError::UnknownCommandType(opcode as char)),
        }
    }

    fn parse_redis_command(&mut self, mut cursor: &mut Cursor<&[u8]>) -> Result<Option<RESPItem>, RESPParseError> {
        if !cursor.has_remaining() {
            return Err(RESPParseError::Incomplete);
        }
        let opcode = cursor.get_u8();
        match opcode {
            b'+' => {
                let line = get_line(&mut cursor)?;
                let str = std::str::from_utf8(line)?;
                Ok(Some(RESPItem::SimpleString(str.to_string())))
            }
            b'-' => {
                let line = get_line(&mut cursor)?;
                let str = std::str::from_utf8(line)?;
                Ok(Some(RESPItem::SimpleError(str.to_string())))
            },
            b':' => {
                let num = get_integer(&mut cursor)?;
                Ok(Some(RESPItem::Integer(num)))
            },
            b'$' => {
                if let Some(bulk_str) = self.parse_bulk_string(&mut cursor)? {
                    Ok(Some(RESPItem::BulkString(bulk_str)))
                } else {
                    Ok(Some(RESPItem::Null))
                }
            },
            b'!' => {
                if let Some(bulk_err) = self.parse_bulk_string(&mut cursor)? {
                    Ok(Some(RESPItem::BulkError(bulk_err)))
                } else {
                    Ok(Some(RESPItem::Null))
                }
            }
            b'*' => Ok(Some(RESPItem::Array(self.parse_aggregate(&mut cursor)?))),
            b'~' => Ok(Some(RESPItem::Set(self.parse_aggregate(&mut cursor)?))),
            b'>' => Ok(Some(RESPItem::Push(self.parse_aggregate(&mut cursor)?))),
            b'_' => {
                let line = get_line(&mut cursor)?;
                if line.len() > 0 {
                    Err(RESPParseError::MalformedNull)
                } else {
                    Ok(Some(RESPItem::Null))
                }
            },
            b'#' => {
                let line = get_line(&mut cursor)?;
                if line.len() != 1 {
                    Err(RESPParseError::MalformedBoolean(std::str::from_utf8(line)?.to_string()))
                } else {
                    match line[0] {
                        b't' => Ok(Some(RESPItem::Boolean(true))),
                        b'f' => Ok(Some(RESPItem::Boolean(false))),
                        oth  => Err(RESPParseError::MalformedBoolean((oth as char).to_string())),
                    }
                }
            },
            b',' => {
                let line = get_line(&mut cursor)?;
                match lexical_core::parse::<f64>(line) {
                    Ok(val) => Ok(Some(RESPItem::Double(val))),
                    Err(_) => Err(RESPParseError::MalformedDouble)
                }
            },
            b'(' => {
                let line = get_line(&mut cursor)?;
                if let Some(bigint) = BigInt::parse_bytes(line, 10) {
                    Ok(Some(RESPItem::BigNumber(bigint)))
                } else {
                    Err(RESPParseError::MalformedInteger)
                }
            },
            b'=' => {
                if let Some(bulk_str) = self.parse_bulk_string(&mut cursor)? {
                    if bulk_str.len() < 4 {
                        return Err(RESPParseError::VerbatimStringMustAtLeastHave4Chars);
                    }
                    let mut parts = bulk_str.splitn(2, ":")
                        .map(|s| s.to_string())
                        .collect::<Vec<String>>();
                    if parts.len() != 2 {
                        return Err(RESPParseError::VerbatimStringFormatMalformed);
                    }
                    Ok(Some(RESPItem::VerbatimString {
                        encoding: parts.swap_remove(0),
                        data: parts.swap_remove(1),
                    }))
                } else {
                    Err(RESPParseError::VerbatimStringMustAtLeastHave4Chars)
                }
            },
            b'%' => {
                let num_entries = get_integer(&mut cursor)?;
                if num_entries < 0 {
                    Err(RESPParseError::NegativeLength)
                } else if num_entries == 0 {
                    Ok(Some(RESPItem::Map(vec![])))
                } else {
                    let mut entries = Vec::with_capacity(num_entries as usize);
                    for i in 0..num_entries as usize {
                        let key_cmd = self.parse_redis_command(&mut cursor)
                            .map_err(|err| RESPParseError::AggregateError(i, Box::new(err)))?
                            .ok_or(RESPParseError::MalformedCommand)?;
                        let val_cmd = self.parse_redis_command(&mut cursor)
                            .map_err(|err| RESPParseError::AggregateError(i, Box::new(err)))?
                            .ok_or(RESPParseError::MalformedCommand)?;
                        entries.push((key_cmd, val_cmd));
                    }
                    Ok(Some(RESPItem::Map(entries)))
                }
            },
            c => Err(RESPParseError::UnknownCommandType(c as char)),
        }
    }

    fn parse_bulk_string(&mut self, mut cursor: &mut Cursor<&[u8]>) -> Result<Option<String>, RESPParseError> {
        let strlen = get_integer(&mut cursor)?;
        if strlen == -1 {
            Ok(None)
        } else if strlen < 0 {
            Err(RESPParseError::NegativeLength)
        } else {
            let line = get_line(&mut cursor)?;
            if line.len() != (strlen as usize) {
                Err(RESPParseError::MalformedBulkStringLengthMismatch)
            } else {
                let str = std::str::from_utf8(line)?;
                return Ok(Some(str.to_string()))
            }
        }
    }

    fn parse_aggregate(&mut self, mut cursor: &mut Cursor<&[u8]>) -> Result<Vec<RESPItem>, RESPParseError> {
        let arr_len = get_integer(&mut cursor)?;
        if arr_len < 0 {
            Err(RESPParseError::NegativeLength)
        } else if arr_len == 0 {
            Ok(vec![])
        } else {
            let mut commands = Vec::with_capacity(arr_len as usize);
            for i in 0..arr_len as usize {
                match self.parse_redis_command(&mut cursor) {
                    Ok(Some(cmd)) => commands.push(cmd),
                    Ok(None) => return Err(RESPParseError::AggregateError(i, Box::new(RESPParseError::MalformedCommand))),
                    Err(inner_cmd) => return Err(RESPParseError::AggregateError(i, Box::new(inner_cmd))),
                }
            }
            Ok(commands)
        }
    }

    fn encode_redis_command(&mut self, item: &RESPItem, dst: &mut BytesMut) -> Result<(), RESPEncodeError> {
        dst.put_u8(item.opcode());
        match item {
            RESPItem::SimpleString(s) => {
                self.encode_simple_string(&s, dst);
            },
            RESPItem::SimpleError(e) => {
                self.encode_simple_string(&e, dst);
            },
            RESPItem::Integer(i) => {
                self.encode_number(*i, dst);
            },
            RESPItem::BulkString(b) => {
                self.encode_bulk_string(&b, dst);
            },
            RESPItem::Array(ref a) => {
                self.encode_aggregate(a, dst)?;
            },
            RESPItem::Null =>{
                self.encode_crlf(dst);
            },
            RESPItem::Boolean(val) => {
                self.encode_simple_string(if *val { "t" } else { "f" }, dst);
            },
            RESPItem::Double(d) => {
                self.encode_number(*d, dst);
            },
            RESPItem::BigNumber(bn) => {
                self.encode_simple_string(bn.to_string().as_ref(), dst);
            },
            RESPItem::BulkError(be) => {
                self.encode_bulk_string(&be, dst);
            },
            RESPItem::VerbatimString { encoding, data } => {
                if encoding.len() != 3 {
                    return Err(RESPEncodeError::VerbatimStringEncodingNameLength);
                }
                let formatted = format!("{}:{}", encoding, data);
                self.encode_bulk_string(&formatted, dst);
            },
            RESPItem::Map(ref m) => {
                self.encode_number(m.len(), dst);
                for (k, v) in m {
                    self.encode_redis_command(k, dst)?;
                    self.encode_redis_command(v, dst)?;
                }
            },
            RESPItem::Set(ref s) => {
                self.encode_aggregate(s, dst)?;
            },
            RESPItem::Push(ref p) => {
                self.encode_aggregate(p, dst)?;
            },
        };
        Ok(())
    }

    fn encode_aggregate(&mut self, items: &Vec<RESPItem>, dst: &mut BytesMut) -> Result<(), RESPEncodeError> {
        self.encode_number(items.len(), dst);
        for resp_item in items {
            self.encode_redis_command(resp_item, dst)?;
        }
        Ok(())
    }

    #[inline]
    fn encode_bulk_string(&mut self, s: &str, dst: &mut BytesMut) {
        self.encode_number(s.len(), dst);
        self.encode_simple_string(s, dst);
    }

    #[inline]
    fn encode_number<N: Num + ToString>(&mut self, i: N, dst: &mut BytesMut) {
        dst.put_slice(i.to_string().as_bytes());
        self.encode_crlf(dst);
    }

    #[inline]
    fn encode_simple_string(&mut self, s: &str, dst: &mut BytesMut) {
        dst.put_slice(s.as_bytes());
        self.encode_crlf(dst);
    }

    #[inline]
    fn encode_crlf(&mut self, dst: &mut BytesMut) {
        dst.put_slice("\r\n".as_bytes())
    }
}

impl Decoder for RESPCodec {
    type Item = RESPItem;
    type Error = RESPParseError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.decode_redis_command(buf)
    }
}

impl Encoder<RESPItem> for RESPCodec {
    type Error = RESPEncodeError;

    fn encode(&mut self, item: RESPItem, dst: &mut BytesMut) -> Result<(), Self::Error> {
        self.encode_redis_command(&item, dst)
    }
}

fn get_integer(buf: &mut Cursor<&[u8]>) -> Result<i64, RESPParseError> {
    let line = get_line(buf)?;
    lexical_core::parse::<i64>(line).map_err(|_| RESPParseError::MalformedInteger)
}

fn get_line<'a>(buf: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], RESPParseError> {
    if !buf.has_remaining() {
        return Err(RESPParseError::Incomplete);
    }
    let start = buf.position() as usize;
    let end = buf.get_ref().len() - 1;
    for i in start..end {
        if buf.get_ref()[i] == b'\r' && buf.get_ref()[i+1] == b'\n' {
            buf.set_position((i+2) as u64);
            return Ok(&buf.get_ref()[start..i]);
        }
    }
    return Err(RESPParseError::Incomplete);
}

#[cfg(test)]
mod redis_decoding {
    use super::*;
    use super::RESPItem::*;
    use super::RESPParseError::*;

    #[test]
    fn test_parse_simple_string() {
        let mut codec = RESPCodec::new();
        let mut buffer = BytesMut::from("+OK\r\n");
        let decoded = codec.decode(&mut buffer);
        assert_eq!(decoded, Ok(Some(SimpleString("OK".to_string()))));
    }

    #[test]
    fn test_parse_simple_error() {
        let mut codec = RESPCodec::new();
        let mut buffer = BytesMut::from("-Error message\r\n");
        let decoded = codec.decode(&mut buffer);
        assert_eq!(decoded, Ok(Some(SimpleError("Error message".to_string()))));
    }

    #[test]
    fn test_parse_correct_integer_with_positive_sign() {
        let mut codec = RESPCodec::new();
        let mut buffer = BytesMut::from(":+100\r\n");
        let decoded = codec.decode(&mut buffer);
        assert_eq!(decoded, Ok(Some(Integer(100))));
    }

    #[test]
    fn test_parse_correct_integer_without_sign() {
        let mut codec = RESPCodec::new();
        let mut buffer = BytesMut::from(":100\r\n");
        let decoded = codec.decode(&mut buffer);
        assert_eq!(decoded, Ok(Some(Integer(100))));
    }

    #[test]
    fn test_parse_zero_integer_correctly() {
        let mut codec = RESPCodec::new();
        let mut buffer = BytesMut::from(":0\r\n");
        let decoded = codec.decode(&mut buffer);
        assert_eq!(decoded, Ok(Some(Integer(0))));
    }

    #[test]
    fn test_parse_correct_integer_negative() {
        let mut codec = RESPCodec::new();
        let mut buffer = BytesMut::from(":-100\r\n");
        let decoded = codec.decode(&mut buffer);
        assert_eq!(decoded, Ok(Some(Integer(-100))));
    }

    #[test]
    fn test_parse_error_on_integer_overflow() {
        let mut codec = RESPCodec::new();
        let mut buffer = BytesMut::from(":+100000000000000000000000000000000000000000000000000\r\n");
        let decoded = codec.decode(&mut buffer);
        assert_eq!(decoded, Err(MalformedInteger));
    }

    #[test]
    fn test_parse_error_on_invalid_integer_format() {
        let mut codec = RESPCodec::new();

        for (name, inv_str) in vec![
            ("double plus", ":++100\r\n"),
            ("has characters", ":abcde\r\n"),
            ("hex is disallowed", ":0xdeadbeef\r\n"),
        ].into_iter() {
            let mut buffer = BytesMut::from(inv_str);
            let decoded = codec.decode(&mut buffer);
            assert_eq!(decoded, Err(MalformedInteger), "failed on {name:?}");
        }
    }

    #[test]
    fn test_parse_well_formed_bulk_string() {
        let mut codec = RESPCodec::new();
        let mut buffer = BytesMut::from("$5\r\nhello\r\n");
        let decoded = codec.decode(&mut buffer);
        assert_eq!(decoded, Ok(Some(BulkString("hello".to_string()))));
    }

    #[test]
    fn test_parse_well_formed_null_bulk_string() {
        let mut codec = RESPCodec::new();
        let mut buffer = BytesMut::from("$-1\r\n");
        let decoded = codec.decode(&mut buffer);
        assert_eq!(decoded, Ok(Some(Null)));
    }

    #[test]
    fn test_parse_well_formed_empty_bulk_string() {
        let mut codec = RESPCodec::new();
        let mut buffer = BytesMut::from("$0\r\n\r\n");
        let decoded = codec.decode(&mut buffer);
        assert_eq!(decoded, Ok(Some(BulkString("".to_string()))));
    }

    #[test]
    fn test_parse_bad_bulk_string_with_invalid_negative_length() {
        let mut codec = RESPCodec::new();
        let mut buffer = BytesMut::from("$-10\r\n");
        let decoded = codec.decode(&mut buffer);
        assert_eq!(decoded, Err(NegativeLength));
    }

    #[test]
    fn test_parse_bad_bulk_string_with_length_mismatch_more_chars() {
        let mut codec = RESPCodec::new();
        let mut buffer = BytesMut::from("$5\r\nhelloo\r\n");
        let decoded = codec.decode(&mut buffer);
        assert_eq!(decoded, Err(MalformedBulkStringLengthMismatch));
    }

    #[test]
    fn test_parse_bad_bulk_string_with_length_mismatch_fewer_chars() {
        let mut codec = RESPCodec::new();
        let mut buffer = BytesMut::from("$5\r\nhell\r\n");
        let decoded = codec.decode(&mut buffer);
        assert_eq!(decoded, Err(MalformedBulkStringLengthMismatch));
    }

    #[test]
    fn test_parse_empty_array() {
        let mut codec = RESPCodec::new();
        let mut buffer = BytesMut::from("*0\r\n");
        let decoded = codec.decode(&mut buffer);
        assert_eq!(decoded, Ok(Some(Array(vec![]))));
    }

    #[test]
    fn test_parse_array_bulk_string_hello_world() {
        let mut codec = RESPCodec::new();
        let mut buffer = BytesMut::from("*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n");
        let decoded = codec.decode(&mut buffer);
        assert_eq!(decoded, Ok(Some(Array(vec![
            BulkString("hello".to_string()),
            BulkString("world".to_string()),
        ]))));
    }

    #[test]
    fn test_parse_array_list_of_integers() {
        let mut codec = RESPCodec::new();
        let mut buffer = BytesMut::from("*3\r\n:1\r\n:2\r\n:3\r\n");
        let decoded = codec.decode(&mut buffer);
        assert_eq!(decoded, Ok(Some(Array(vec![Integer(1), Integer(2), Integer(3)]))));
    }

    #[test]
    fn test_parse_array_of_mixed_data_types() {
        let mut codec = RESPCodec::new();
        let mut buffer = BytesMut::from(concat!(
            "*5\r\n",
            ":1\r\n",
            ":2\r\n",
            ":3\r\n",
            ":4\r\n",
            "$5\r\nhello\r\n",
        ));
        let decoded = codec.decode(&mut buffer);
        assert_eq!(decoded, Ok(Some(Array(vec![
            Integer(1), Integer(2), Integer(3), Integer(4),
            BulkString("hello".to_string()),
        ]))));
    }

    #[test]
    fn test_parse_boolean() {
        let mut codec = RESPCodec::new();
        for (name, redis_str, expected) in vec![
            ("true", "#t\r\n", Ok(Some(Boolean(true)))),
            ("false", "#f\r\n", Ok(Some(Boolean(false)))),
            ("non-sense", "#x\r\n", Err(MalformedBoolean("x".to_string()))),
            ("non-sense multi-char", "#tf\r\n", Err(MalformedBoolean("tf".to_string()))),
        ].into_iter() {
            let mut buffer = BytesMut::from(redis_str);
            let decoded = codec.decode(&mut buffer);
            assert_eq!(decoded, expected, "failed test {name:?}")
        }
    }

    #[test]
    fn test_parse_double() {
        let mut codec = RESPCodec::new();
        for (name, redis_str, expected) in vec![
            ("positive without fractional part - positive integer", ",10\r\n", Ok(Some(Double(10.0)))),
            ("negative without fractional part - negative integer", ",-10\r\n", Ok(Some(Double(-10.0)))),
            ("zero value - integer representation", ",0\r\n", Ok(Some(Double(0.0)))),
            ("zero value - float representation", ",0.0\r\n", Ok(Some(Double(0.0)))),
            ("positive with fractional part in decimal notation", ",1.23\r\n", Ok(Some(Double(1.23)))),
            ("negative with fraction part in decimal notation", ",-1.23\r\n", Ok(Some(Double(-1.23)))),
            ("positive with positive exponent", ",1.23e2\r\n", Ok(Some(Double(1.23e2)))),
            ("positive with negative exponent", ",1.23e-2\r\n", Ok(Some(Double(1.23e-2)))),
            ("positive with positive EXPONENT", ",1.23E2\r\n", Ok(Some(Double(1.23e2)))),
            ("positive with negative EXPONENT", ",1.23E-2\r\n", Ok(Some(Double(1.23e-2)))),
            ("positive infinity", ",inf\r\n", Ok(Some(Double(f64::INFINITY)))),
            ("negative infinity", ",-inf\r\n", Ok(Some(Double(f64::NEG_INFINITY)))),
        ] {
            let mut buffer = BytesMut::from(redis_str);
            let decoded = codec.decode(&mut buffer);
            assert_eq!(decoded, expected, "failed test {name:?}")
        }
    }

    #[test]
    fn test_parse_double_nan() {
        let mut buffer = BytesMut::from(",nan\r\n");
        let mut codec = RESPCodec::new();
        let decoded = codec.decode(&mut buffer);
        assert!(match decoded.unwrap().unwrap() {
            Double(x) if x.is_nan() => true,
            _ => false,
        })
    }

    #[test]
    fn test_positive_bigint() {

    }

    #[test]
    fn test_negative_bigint() {

    }

    #[test]
    fn test_invalid_bigint_representations() {

    }
}