use std::fmt::{Display, Formatter};
use std::io::{Cursor, Error, ErrorKind};
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
pub enum RESPFrame {
    SimpleString(String),          // +
    SimpleError(String),           // -
    Integer(i64),                  // :
    BulkString(String),            // $
    Array(Vec<RESPFrame>),          // *
    Null,                          // _
    Boolean(bool),                 // #
    Double(f64),                   // ,
    BigNumber(BigInt),             // (
    BulkError(String),             // !
    VerbatimString{
        encoding: String,
        data: String,
    },                              // =
    Map(Vec<(RESPFrame, RESPFrame)>), // %
    Set(Vec<RESPFrame>),             // ~
    Push(Vec<RESPFrame>),            // >
}

impl RESPFrame {
    pub fn opcode(&self) -> u8 {
        match self {
            RESPFrame::SimpleString(_) => RESPOpcode::SIMPLE_STRING,
            RESPFrame::SimpleError(_) => RESPOpcode::SIMPLE_ERROR,
            RESPFrame::Integer(_) => RESPOpcode::INTEGER,
            RESPFrame::BulkString(_) => RESPOpcode::BULK_STRING,
            RESPFrame::Array(_) => RESPOpcode::ARRAY,
            RESPFrame::Null => RESPOpcode::NULL,
            RESPFrame::Boolean(_) => RESPOpcode::BOOLEAN,
            RESPFrame::Double(_) => RESPOpcode::DOUBLE,
            RESPFrame::BigNumber(_) => RESPOpcode::BIG_NUMBER,
            RESPFrame::BulkError(_) => RESPOpcode::BULK_ERROR,
            RESPFrame::VerbatimString { .. } => RESPOpcode::VERBATIM_STRING,
            RESPFrame::Map(_) => RESPOpcode::MAP,
            RESPFrame::Set(_) => RESPOpcode::SET,
            RESPFrame::Push(_) => RESPOpcode::PUSH,
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum RESPConversionError {
    TypeMismatchError
}

impl TryInto<String> for RESPFrame {
    type Error = RESPConversionError;

    fn try_into(self) -> Result<String, Self::Error> {
        match self {
            RESPFrame::SimpleString(s) => Ok(s),
            RESPFrame::BulkString(s) => Ok(s),
            _ => Err(RESPConversionError::TypeMismatchError),
        }
    }
}

impl TryInto<i64> for RESPFrame {
    type Error = RESPConversionError;

    fn try_into(self) -> Result<i64, Self::Error> {
        match self {
            RESPFrame::Integer(i) => Ok(i),
            _ => Err(RESPConversionError::TypeMismatchError),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum RESPParseError {
    IOError(String),
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
    MalformedUtf8String(Utf8Error),
    MalformedCommand,
    AggregateError(usize, Box<RESPParseError>),
}

impl From<Error> for RESPParseError {
    fn from(value: Error) -> Self {
        RESPParseError::IOError(value.to_string())
    }
}

impl From<Utf8Error> for RESPParseError {
    fn from(inner: Utf8Error) -> Self {
        RESPParseError::MalformedUtf8String(inner)
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum RESPEncodeError {
    Inner(String),
    VerbatimStringEncodingNameLength,
}

impl From<RESPEncodeError> for Error {
    fn from(value: RESPEncodeError) -> Self {
        Error::new(ErrorKind::Other, format!("{value:?}"))
    }
}

impl From<Error> for RESPEncodeError {
    fn from(value: Error) -> Self {
        RESPEncodeError::Inner(value.to_string())
    }
}

impl Display for RESPEncodeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&*self.to_string())
    }
}

impl std::error::Error for RESPEncodeError {}

impl RESPCodec {
    pub fn new() -> Self {
        RESPCodec {}
    }

    fn decode_redis_command(&mut self, buf: &mut BytesMut) -> Result<Option<RESPFrame>, RESPParseError> {
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

    fn parse_redis_command(&mut self, mut cursor: &mut Cursor<&[u8]>) -> Result<Option<RESPFrame>, RESPParseError> {
        if !cursor.has_remaining() {
            return Err(RESPParseError::Incomplete);
        }
        let opcode = cursor.get_u8();
        match opcode {
            RESPOpcode::SIMPLE_STRING => {
                let line = get_line(&mut cursor)?;
                let str = std::str::from_utf8(line)?;
                Ok(Some(RESPFrame::SimpleString(str.to_string())))
            }
            RESPOpcode::SIMPLE_ERROR => {
                let line = get_line(&mut cursor)?;
                let str = std::str::from_utf8(line)?;
                Ok(Some(RESPFrame::SimpleError(str.to_string())))
            },
            RESPOpcode::INTEGER => {
                let num = get_integer(&mut cursor)?;
                Ok(Some(RESPFrame::Integer(num)))
            },
            RESPOpcode::BULK_STRING => {
                if let Some(bulk_str) = self.parse_bulk_string(&mut cursor)? {
                    Ok(Some(RESPFrame::BulkString(bulk_str)))
                } else {
                    Ok(Some(RESPFrame::Null))
                }
            },
            RESPOpcode::BULK_ERROR => {
                if let Some(bulk_err) = self.parse_bulk_string(&mut cursor)? {
                    Ok(Some(RESPFrame::BulkError(bulk_err)))
                } else {
                    Ok(Some(RESPFrame::Null))
                }
            }
            RESPOpcode::ARRAY => Ok(Some(RESPFrame::Array(self.parse_aggregate(&mut cursor)?))),
            RESPOpcode::SET => Ok(Some(RESPFrame::Set(self.parse_aggregate(&mut cursor)?))),
            RESPOpcode::PUSH => Ok(Some(RESPFrame::Push(self.parse_aggregate(&mut cursor)?))),
            RESPOpcode::NULL => {
                let line = get_line(&mut cursor)?;
                if line.len() > 0 {
                    Err(RESPParseError::MalformedNull)
                } else {
                    Ok(Some(RESPFrame::Null))
                }
            },
            RESPOpcode::BOOLEAN => {
                let line = get_line(&mut cursor)?;
                if line.len() != 1 {
                    Err(RESPParseError::MalformedBoolean(std::str::from_utf8(line)?.to_string()))
                } else {
                    match line[0] {
                        b't' => Ok(Some(RESPFrame::Boolean(true))),
                        b'f' => Ok(Some(RESPFrame::Boolean(false))),
                        oth  => Err(RESPParseError::MalformedBoolean((oth as char).to_string())),
                    }
                }
            },
            RESPOpcode::DOUBLE => {
                let line = get_line(&mut cursor)?;
                match lexical_core::parse::<f64>(line) {
                    Ok(val) => Ok(Some(RESPFrame::Double(val))),
                    Err(_) => Err(RESPParseError::MalformedDouble)
                }
            },
            RESPOpcode::BIG_NUMBER => {
                let line = get_line(&mut cursor)?;
                if let Some(bigint) = BigInt::parse_bytes(line, 10) {
                    Ok(Some(RESPFrame::BigNumber(bigint)))
                } else {
                    Err(RESPParseError::MalformedInteger)
                }
            },
            RESPOpcode::VERBATIM_STRING => {
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
                    let encoding = parts.swap_remove(0);
                    let data = parts.swap_remove(0);
                    Ok(Some(RESPFrame::VerbatimString { encoding, data }))
                } else {
                    Err(RESPParseError::VerbatimStringMustAtLeastHave4Chars)
                }
            },
            RESPOpcode::MAP => {
                let num_entries = get_integer(&mut cursor)?;
                if num_entries < 0 {
                    Err(RESPParseError::NegativeLength)
                } else if num_entries == 0 {
                    Ok(Some(RESPFrame::Map(vec![])))
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
                    Ok(Some(RESPFrame::Map(entries)))
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

    fn parse_aggregate(&mut self, mut cursor: &mut Cursor<&[u8]>) -> Result<Vec<RESPFrame>, RESPParseError> {
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

    fn encode_redis_command(&mut self, item: &RESPFrame, dst: &mut BytesMut) -> Result<(), RESPEncodeError> {
        dst.put_u8(item.opcode());
        match item {
            RESPFrame::SimpleString(s) => {
                self.encode_simple_string(&s, dst);
            },
            RESPFrame::SimpleError(e) => {
                self.encode_simple_string(&e, dst);
            },
            RESPFrame::Integer(i) => {
                self.encode_number(*i, dst);
            },
            RESPFrame::BulkString(b) => {
                self.encode_bulk_string(&b, dst);
            },
            RESPFrame::Array(ref a) => {
                self.encode_aggregate(a, dst)?;
            },
            RESPFrame::Null =>{
                self.encode_crlf(dst);
            },
            RESPFrame::Boolean(val) => {
                self.encode_simple_string(if *val { "t" } else { "f" }, dst);
            },
            RESPFrame::Double(d) => {
                self.encode_number(*d, dst);
            },
            RESPFrame::BigNumber(bn) => {
                self.encode_simple_string(bn.to_string().as_ref(), dst);
            },
            RESPFrame::BulkError(be) => {
                self.encode_bulk_string(&be, dst);
            },
            RESPFrame::VerbatimString { encoding, data } => {
                if encoding.len() != 3 {
                    return Err(RESPEncodeError::VerbatimStringEncodingNameLength);
                }
                let formatted = format!("{}:{}", encoding, data);
                self.encode_bulk_string(&formatted, dst);
            },
            RESPFrame::Map(ref m) => {
                self.encode_number(m.len(), dst);
                for (k, v) in m {
                    self.encode_redis_command(k, dst)?;
                    self.encode_redis_command(v, dst)?;
                }
            },
            RESPFrame::Set(ref s) => {
                self.encode_aggregate(s, dst)?;
            },
            RESPFrame::Push(ref p) => {
                self.encode_aggregate(p, dst)?;
            },
        };
        Ok(())
    }

    fn encode_aggregate(&mut self, items: &Vec<RESPFrame>, dst: &mut BytesMut) -> Result<(), RESPEncodeError> {
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
        dst.put_slice(i.to_string().to_lowercase().as_bytes());
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
    type Item = RESPFrame;
    type Error = RESPParseError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.decode_redis_command(buf)
    }
}

impl Encoder<RESPFrame> for RESPCodec {
    type Error = RESPEncodeError;

    fn encode(&mut self, item: RESPFrame, dst: &mut BytesMut) -> Result<(), Self::Error> {
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
mod resp_decoding {
    use super::*;
    use super::RESPFrame::*;
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
        let mut buffer = BytesMut::from("(100000000000000000000000000000000000000000000000\r\n");
        let mut codec = RESPCodec::new();
        let decoded = codec.decode(&mut buffer);
        assert_eq!(Ok(Some(BigNumber(BigInt::parse_bytes("100000000000000000000000000000000000000000000000".as_bytes(), 10).unwrap()))), decoded);
    }

    #[test]
    fn test_negative_bigint() {
        let mut buffer = BytesMut::from("(-100000000000000000000000000000000000000000000000\r\n");
        let mut codec = RESPCodec::new();
        let decoded = codec.decode(&mut buffer);
        assert_eq!(Ok(Some(BigNumber(BigInt::parse_bytes("-100000000000000000000000000000000000000000000000".as_bytes(), 10).unwrap()))), decoded);
    }

    #[test]
    fn test_invalid_bigint_representations() {
        let mut buffer = BytesMut::from("(100000000000000000001001010000100001000100010000000xdead\r\n");
        let mut codec = RESPCodec::new();
        let decoded = codec.decode(&mut buffer);
        assert_eq!(Err(MalformedInteger), decoded);
    }

    #[test]
    fn test_verbatim_string_correct_encoding() {
        let mut buffer = BytesMut::from("=14\r\ntxt:helloworld\r\n");
        let mut codec = RESPCodec::new();
        let decoded = codec.decode(&mut buffer);
        assert_eq!(Ok(Some(VerbatimString{encoding:"txt".to_string(), data:"helloworld".to_string()})), decoded);
    }

    #[test]
    fn test_invalid_verbatim_string_no_encoding_specified() {
        let mut buffer = BytesMut::from("=10\r\nhelloworld\r\n");
        let mut codec = RESPCodec::new();
        let decoded = codec.decode(&mut buffer);
        assert_eq!(Err(VerbatimStringFormatMalformed), decoded);
    }

    #[test]
    fn test_invalid_verbatim_string_encoding_not_equal_to_3_chars() {
        let mut buffer = BytesMut::from("=10\r\nxy:loworld\r\n");
        let mut codec = RESPCodec::new();
        let decoded = codec.decode(&mut buffer);
        assert_eq!(Err(VerbatimStringFormatMalformed), decoded);
    }

    #[test]
    fn test_invalid_verbatim_string_less_than_4_chars() {
        let mut buffer = BytesMut::from("=3\r\nhel\r\n");
        let mut codec = RESPCodec::new();
        let decoded = codec.decode(&mut buffer);
        assert_eq!(Err(VerbatimStringMustAtLeastHave4Chars), decoded);
    }

    #[test]
    fn test_parse_valid_set() {
        let mut buffer = BytesMut::from("~2\r\n+ok\r\n:100\r\n");
        let mut codec = RESPCodec::new();
        let decoded = codec.decode(&mut buffer);
        assert_eq!(Ok(Some(Set(vec![SimpleString("ok".to_string()), Integer(100)]))), decoded);
    }

    #[test]
    fn test_parse_valid_push() {
        let mut buffer = BytesMut::from(">2\r\n:100\r\n:200\r\n");
        let mut codec = RESPCodec::new();
        let decoded = codec.decode(&mut buffer);
        assert_eq!(Ok(Some(Push(vec![Integer(100), Integer(200)]))), decoded);
    }

    #[test]
    fn test_parse_valid_map() {
        let mut buffer = BytesMut::from("%2\r\n+foo\r\n:100\r\n+bar\r\n:200\r\n");
        let mut codec = RESPCodec::new();
        let decoded = codec.decode(&mut buffer);
        assert_eq!(Ok(Some(Map(vec![
            (SimpleString("foo".to_string()), Integer(100)),
            (SimpleString("bar".to_string()), Integer(200)),
        ]))), decoded);
    }

    #[test]
    fn test_parse_invalid_map_with_odd_number_of_items() {
        let buffer = BytesMut::from("%2\r\n+foo\r\n:100\r\n+bar\r\n");
        let mut codec = RESPCodec::new();
        let mut cursor = Cursor::new(&buffer[..]);
        let decoded = codec.parse_redis_command(&mut cursor);
        assert_eq!(Err(AggregateError(1, Box::new(Incomplete))), decoded);
    }
}

#[cfg(test)]
mod resp_encoding {
    use std::str::from_utf8;

    use crate::resp_codec::RESPEncodeError::VerbatimStringEncodingNameLength;

    use super::*;
    use super::RESPFrame::*;

    #[test]
    fn test_encode_simple_string() {
        let resp = SimpleString("abc".to_string());
        let mut encoder = RESPCodec::new();
        let mut buff = BytesMut::new();
        let res = encoder.encode(resp, &mut buff);
        assert!(res.is_ok());
        assert_eq!(from_utf8(buff.as_ref()).unwrap(), "+abc\r\n");
    }

    #[test]
    fn test_encode_simple_error() {
        let resp = SimpleError("error".to_string());
        let mut encoder = RESPCodec::new();
        let mut buff = BytesMut::new();
        let res = encoder.encode(resp, &mut buff);
        assert!(res.is_ok());
        assert_eq!(from_utf8(buff.as_ref()).unwrap(), "-error\r\n");
    }

    #[test]
    fn test_encode_bulk_string() {
        let resp = BulkString("abc".to_string());
        let mut encoder = RESPCodec::new();
        let mut buff = BytesMut::new();
        let res = encoder.encode(resp, &mut buff);
        assert!(res.is_ok());
        assert_eq!(from_utf8(buff.as_ref()).unwrap(), "$3\r\nabc\r\n");
    }

    #[test]
    fn test_encode_bulk_error() {
        let resp = BulkError("abc".to_string());
        let mut encoder = RESPCodec::new();
        let mut buff = BytesMut::new();
        let res = encoder.encode(resp, &mut buff);
        assert!(res.is_ok());
        assert_eq!(from_utf8(buff.as_ref()).unwrap(), "!3\r\nabc\r\n");
    }

    #[test]
    fn test_encode_array_with_mixed_data_types() {
        let resp = Array(vec![
            Integer(100),
            SimpleString("abc".to_string()),
            BulkString("def".to_string()),
        ]);
        let mut encoder = RESPCodec::new();
        let mut buff = BytesMut::new();
        let res = encoder.encode(resp, &mut buff);
        assert!(res.is_ok());
        assert_eq!(from_utf8(buff.as_ref()).unwrap(),
                   "*3\r\n:100\r\n+abc\r\n$3\r\ndef\r\n");
    }

    #[test]
    fn test_encode_array_with_single_data_types() {
        let resp = Array(vec![Integer(100), Integer(200), Integer(300)]);
        let mut encoder = RESPCodec::new();
        let mut buff = BytesMut::new();
        let res = encoder.encode(resp, &mut buff);
        assert!(res.is_ok());
        assert_eq!(from_utf8(buff.as_ref()).unwrap(),
                   "*3\r\n:100\r\n:200\r\n:300\r\n");
    }

    #[test]
    fn test_encode_array_with_nested_aggregate_types() {
        let resp = Array(vec![
            Array(vec![Integer(100), Double(1.5)]),
            Array(vec![
                SimpleString("ok".to_string()),
                SimpleError("error".to_string()),
            ]),
        ]);
        let mut encoder = RESPCodec::new();
        let mut buff = BytesMut::new();
        let res = encoder.encode(resp, &mut buff);
        assert!(res.is_ok());
        assert_eq!(from_utf8(buff.as_ref()).unwrap(),
                   "*2\r\n*2\r\n:100\r\n,1.5\r\n*2\r\n+ok\r\n-error\r\n");
    }

    #[test]
    fn test_encode_set_with_simple_data() {
        let resp = Set(vec![SimpleString("a".to_string()), SimpleString("b".to_string())]);
        let mut encoder = RESPCodec::new();
        let mut buff = BytesMut::new();
        let res = encoder.encode(resp, &mut buff);
        assert!(res.is_ok());
        assert_eq!(from_utf8(buff.as_ref()).unwrap(), "~2\r\n+a\r\n+b\r\n");
    }

    #[test]
    fn test_encode_map_with_simple_data() {
        let resp = Map(vec![
            (Integer(100), SimpleString("a".to_string())),
            (Integer(200), SimpleString("b".to_string())),
        ]);
        let mut encoder = RESPCodec::new();
        let mut buff = BytesMut::new();
        let res = encoder.encode(resp, &mut buff);
        assert!(res.is_ok());
        assert_eq!(from_utf8(buff.as_ref()).unwrap(), "%2\r\n:100\r\n+a\r\n:200\r\n+b\r\n");
    }

    #[test]
    fn test_encode_map_with_heterogeneous_data() {
        let resp = Map(vec![
            (Integer(100), SimpleString("a".to_string())),
            (SimpleString("ok".to_string()), Array(vec![Integer(10), Integer(20)])),
        ]);
        let mut encoder = RESPCodec::new();
        let mut buff = BytesMut::new();
        let res = encoder.encode(resp, &mut buff);
        assert!(res.is_ok());
        assert_eq!(from_utf8(buff.as_ref()).unwrap(),
                   "%2\r\n:100\r\n+a\r\n+ok\r\n*2\r\n:10\r\n:20\r\n");
    }

    #[test]
    fn test_encode_simple_types() {
        let test_cases = vec![
            ("positive", Integer(100), ":100\r\n"),
            ("zero",     Integer(0),   ":0\r\n"),
            ("negative", Integer(-20), ":-20\r\n"),

            ("positive", Double(1.4),  ",1.4\r\n"),
            ("zero",     Double(0.0),  ",0\r\n"),
            ("negative", Double(-1.3), ",-1.3\r\n"),
            ("+infinity", Double(f64::INFINITY), ",inf\r\n"),
            ("-infinity", Double(f64::NEG_INFINITY), ",-inf\r\n"),
            ("nan", Double(f64::NAN), ",nan\r\n"),

            ("big positive", BigNumber(BigInt::from(1000)), "(1000\r\n"),
            ("big negative", BigNumber(BigInt::from(-100)), "(-100\r\n"),
            ("big zero", BigNumber(BigInt::from(0)), "(0\r\n"),

            ("boolean true", Boolean(true), "#t\r\n"),
            ("boolean false", Boolean(false), "#f\r\n"),

            ("null", Null, "_\r\n"),
        ];
        let mut encoder = RESPCodec::new();
        for (test_name, resp_frame, expected_output) in test_cases.into_iter() {
            let mut buff = BytesMut::new();
            let res = encoder.encode(resp_frame, &mut buff);
            assert!(res.is_ok(), "{test_name:?}");
            assert_eq!(from_utf8(buff.as_ref()).unwrap(), expected_output, "{expected_output:?}");
        }
    }

    #[test]
    fn test_encode_valid_verbatim_string() {
        let resp = VerbatimString {
            encoding: "txt".to_string(),
            data:     "hello".to_string(),
        };
        let mut encoder = RESPCodec::new();
        let mut buff = BytesMut::new();
        let res = encoder.encode(resp, &mut buff);
        assert!(res.is_ok());
        assert_eq!(from_utf8(buff.as_ref()).unwrap(), "=9\r\ntxt:hello\r\n");
    }

    #[test]
    fn test_encode_should_error_when_verbatim_string_format_is_missing() {
        let resp = VerbatimString {
            encoding: "".to_string(),
            data:     "hello".to_string(),
        };
        let mut encoder = RESPCodec::new();
        let mut buff = BytesMut::new();
        let res = encoder.encode(resp, &mut buff);
        assert_eq!(Err(VerbatimStringEncodingNameLength), res);
    }

    #[test]
    fn test_encode_should_error_when_verbatim_string_format_is_not_3_chars() {
        let resp = VerbatimString{
            encoding: "ab".to_string(),
            data: "hello".to_string()
        };
        let mut encoder = RESPCodec::new();
        let mut buff = BytesMut::new();
        let res = encoder.encode(resp, &mut buff);
        assert_eq!(Err(VerbatimStringEncodingNameLength), res);
    }
}
