use std::io::{Cursor, Error};
use std::str::Utf8Error;

use num_bigint::BigInt;
use tokio_util::bytes::{Buf, BytesMut};
use tokio_util::codec::Decoder;

pub struct RedisCodec;

#[derive(Debug, PartialEq)]
pub enum RedisCommand {
    SimpleString(String),          // +
    SimpleError(String),           // -
    Integer(i64),                  // :
    BulkString(String),            // $
    Array(Vec<RedisCommand>),      // *
    Null,                          // _
    Boolean(bool),                 // #
    Double(f64),                   // ,
    BigNumber(BigInt),             // (
    BulkError(String),             // !
    VerbatimString{
        encoding: String,
        data: String,
    },                                    // =
    Map(Vec<(RedisCommand, RedisCommand)>), // %
    Set(Vec<RedisCommand>),                 // ~
    Push(Vec<RedisCommand>),                // >
}

#[derive(Debug, Eq, PartialEq)]
pub enum RedisParseError {
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
    AggregateError(usize, Box<RedisParseError>),
}

impl From<Error> for RedisParseError {
    fn from(value: Error) -> Self {
        RedisParseError::Inner(value.to_string())
    }
}

impl From<Utf8Error> for RedisParseError {
    fn from(_value: Utf8Error) -> Self {
        RedisParseError::MalformedUtf8String
    }
}

impl RedisCodec {
    pub fn new() -> Self {
        RedisCodec {}
    }

    fn decode_redis_command(&mut self, buf: &mut BytesMut) -> Result<Option<RedisCommand>, RedisParseError> {
        let mut cursor = Cursor::new(&buf[..]);
        match self.check_redis_command(&mut cursor) {
            Ok(()) => {
                cursor.set_position(0);
                self.parse_redis_command(&mut cursor)
            },
            Err(RedisParseError::Incomplete) => Ok(None),
            Err(err) => Err(err),
        }
    }

    fn check_redis_command(&mut self, buf: &mut Cursor<&[u8]>) -> Result<(), RedisParseError> {
        if !buf.has_remaining() {
            return Err(RedisParseError::Incomplete);
        }
        let opcode = buf.get_u8();
        match opcode {
            b'+' | b'-' => {
                let _ = get_line(buf)?;
                Ok(())
            },
            b':' => {
                let _ = get_integer(buf)?;
                Ok(())
            },
            b'$' | b'!' => {
                let str_len = get_integer(buf)?;
                if str_len < -1 {
                    Err(RedisParseError::NegativeLength)
                } else if str_len == -1 {
                    Ok(())
                } else {
                    let str = get_line(buf)?;
                    if str.len() != str_len as usize {
                        Err(RedisParseError::MalformedBulkStringLengthMismatch)
                    } else {
                        Ok(())
                    }
                }
            },
            b'*' | b'~' | b'>' => {
                let arr_len = get_integer(buf)?;
                if arr_len < 0 {
                    Err(RedisParseError::MalformedArrayNegativeLength)
                } else {
                    let mut elem_cnt = 0;
                    while elem_cnt < arr_len {
                        self.check_redis_command(buf)?;
                        elem_cnt += 1;
                    }
                    return Ok(());
                }
            },
            b'_' => {
                let s = get_line(buf)?;
                if !s.is_empty() {
                    Err(RedisParseError::MalformedNull)
                } else {
                    Ok(())
                }
            },
            b'#' => {
                let s = get_line(buf)?;
                if s.len() != 1 {
                    let invalid_bool_val = std::str::from_utf8(s)?.to_string();
                    Err(RedisParseError::MalformedBoolean(invalid_bool_val))
                } else {
                    Ok(())
                }
            },
            b',' => {
                let _s = get_line(buf)?;
                Ok(())
            },
            b'(' => {
                let _s = get_line(buf)?;
                Ok(())
            },
            b'=' => {
                let str_len = get_integer(buf)?;
                if str_len < -1 {
                    Err(RedisParseError::NegativeLength)
                } else {
                    let str = get_line(buf)?;
                    if str.len() != str_len as usize {
                        Err(RedisParseError::MalformedBulkStringLengthMismatch)
                    } else if str.len() < 4 {
                        Err(RedisParseError::VerbatimStringMustAtLeastHave4Chars)
                    } else if str[3] != b':' {
                        Err(RedisParseError::VerbatimStringFormatMalformed)
                    } else {
                        Ok(())
                    }
                }
            },
            b'%' => {
                let num_key_values = get_integer(buf)?;
                if num_key_values < 0 {
                    Err(RedisParseError::NegativeLength)
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
            _ => Err(RedisParseError::UnknownCommandType(opcode as char)),
        }
    }

    fn parse_redis_command(&mut self, mut cursor: &mut Cursor<&[u8]>) -> Result<Option<RedisCommand>, RedisParseError> {
        if !cursor.has_remaining() {
            return Err(RedisParseError::Incomplete);
        }
        let opcode = cursor.get_u8();
        match opcode {
            b'+' => {
                let line = get_line(&mut cursor)?;
                let str = std::str::from_utf8(line)?;
                Ok(Some(RedisCommand::SimpleString(str.to_string())))
            }
            b'-' => {
                let line = get_line(&mut cursor)?;
                let str = std::str::from_utf8(line)?;
                Ok(Some(RedisCommand::SimpleError(str.to_string())))
            },
            b':' => {
                let num = get_integer(&mut cursor)?;
                Ok(Some(RedisCommand::Integer(num)))
            },
            b'$' => {
                if let Some(bulk_str) = self.parse_bulk_string(&mut cursor)? {
                    Ok(Some(RedisCommand::BulkString(bulk_str)))
                } else {
                    Ok(Some(RedisCommand::Null))
                }
            },
            b'!' => {
                if let Some(bulk_err) = self.parse_bulk_string(&mut cursor)? {
                    Ok(Some(RedisCommand::BulkError(bulk_err)))
                } else {
                    Ok(Some(RedisCommand::Null))
                }
            }
            b'*' => Ok(Some(RedisCommand::Array(self.parse_aggregate(&mut cursor)?))),
            b'~' => Ok(Some(RedisCommand::Set(self.parse_aggregate(&mut cursor)?))),
            b'>' => Ok(Some(RedisCommand::Push(self.parse_aggregate(&mut cursor)?))),
            b'_' => {
                let line = get_line(&mut cursor)?;
                if line.len() > 0 {
                    Err(RedisParseError::MalformedNull)
                } else {
                    Ok(Some(RedisCommand::Null))
                }
            },
            b'#' => {
                let line = get_line(&mut cursor)?;
                if line.len() != 1 {
                    Err(RedisParseError::MalformedBoolean(std::str::from_utf8(line)?.to_string()))
                } else {
                    match line[0] {
                        b't' => Ok(Some(RedisCommand::Boolean(true))),
                        b'f' => Ok(Some(RedisCommand::Boolean(false))),
                        oth  => Err(RedisParseError::MalformedBoolean((oth as char).to_string())),
                    }
                }
            },
            b',' => {
                let line = get_line(&mut cursor)?;
                match lexical_core::parse::<f64>(line) {
                    Ok(val) => Ok(Some(RedisCommand::Double(val))),
                    Err(_) => Err(RedisParseError::MalformedDouble)
                }
            },
            b'(' => {
                let line = get_line(&mut cursor)?;
                if let Some(bigint) = BigInt::parse_bytes(line, 10) {
                    Ok(Some(RedisCommand::BigNumber(bigint)))
                } else {
                    Err(RedisParseError::MalformedInteger)
                }
            },
            b'=' => {
                if let Some(bulk_str) = self.parse_bulk_string(&mut cursor)? {
                    if bulk_str.len() < 4 {
                        return Err(RedisParseError::VerbatimStringMustAtLeastHave4Chars);
                    }
                    if bulk_str[3] != ':' {
                        return Err(RedisParseError::VerbatimStringFormatMalformed)
                    }
                    let mut parts = bulk_str.splitn(2, ":").collect::<Vec<String>>();
                    if parts.len() != 2 {
                        return Err(RedisParseError::VerbatimStringFormatMalformed);
                    }
                    Ok(Some(RedisCommand::VerbatimString {
                        encoding: parts.swap_remove(0),
                        data: parts.swap_remove(1),
                    }))
                } else {
                    Err(RedisParseError::VerbatimStringMustAtLeastHave4Chars)
                }
            },
            b'%' => {
                let num_entries = get_integer(&mut cursor)?;
                if num_entries < 0 {
                    Err(RedisParseError::NegativeLength)
                } else if num_entries == 0 {
                    Ok(Some(RedisCommand::Map(vec![])))
                } else {
                    let mut entries = Vec::with_capacity(num_entries as usize);
                    for i in 0..num_entries as usize {
                        let key_cmd = self.parse_redis_command(&mut cursor)
                            .map_err(|err| RedisParseError::AggregateError(i, Box::new(err)))?
                            .ok_or(RedisParseError::MalformedCommand)?;
                        let val_cmd = self.parse_redis_command(&mut cursor)
                            .map_err(|err| RedisParseError::AggregateError(i, Box::new(err)))?
                            .ok_or(RedisParseError::MalformedCommand)?;
                        entries.push((key_cmd, val_cmd));
                    }
                    Ok(Some(RedisCommand::Map(entries)))
                }
            },
            c => Err(RedisParseError::UnknownCommandType(c as char)),
        }
    }

    fn parse_bulk_string(&mut self, mut cursor: &mut Cursor<&[u8]>) -> Result<Option<String>, RedisParseError> {
        let strlen = get_integer(&mut cursor)?;
        if strlen == -1 {
            Ok(None)
        } else if strlen < 0 {
            Err(RedisParseError::NegativeLength)
        } else {
            let line = get_line(&mut cursor)?;
            if line.len() != (strlen as usize) {
                Err(RedisParseError::MalformedBulkStringLengthMismatch)
            } else {
                let str = std::str::from_utf8(line)?;
                return Ok(Some(str.to_string()))
            }
        }
    }

    fn parse_aggregate(&mut self, mut cursor: &mut Cursor<&[u8]>) -> Result<Vec<RedisCommand>, RedisParseError> {
        let arr_len = get_integer(&mut cursor)?;
        if arr_len < 0 {
            Err(RedisParseError::NegativeLength)
        } else if arr_len == 0 {
            Ok(vec![])
        } else {
            let mut commands = Vec::with_capacity(arr_len as usize);
            for i in 0..arr_len as usize {
                match self.parse_redis_command(&mut cursor) {
                    Ok(Some(cmd)) => commands.push(cmd),
                    Ok(None) => return Err(RedisParseError::AggregateError(i, Box::new(RedisParseError::MalformedCommand))),
                    Err(inner_cmd) => return Err(RedisParseError::AggregateError(i, Box::new(inner_cmd))),
                }
            }
            Ok(commands)
        }
    }
}

fn get_integer(buf: &mut Cursor<&[u8]>) -> Result<i64, RedisParseError> {
    let line = get_line(buf)?;
    lexical_core::parse::<i64>(line).map_err(|_| RedisParseError::MalformedInteger)
}

fn get_line<'a>(buf: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], RedisParseError> {
    if !buf.has_remaining() {
        return Err(RedisParseError::Incomplete);
    }
    let start = buf.position() as usize;
    let end = buf.get_ref().len() - 1;
    for i in start..end {
        if buf.get_ref()[i] == b'\r' && buf.get_ref()[i+1] == b'\n' {
            buf.set_position((i+2) as u64);
            return Ok(&buf.get_ref()[start..i]);
        }
    }
    return Err(RedisParseError::Incomplete);
}

impl Decoder for RedisCodec {
    type Item = RedisCommand;
    type Error = RedisParseError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.decode_redis_command(buf)
    }
}

#[cfg(test)]
mod redis_decoding {
    use super::*;
    use super::RedisCommand::*;
    use super::RedisParseError::*;

    #[test]
    fn test_parse_simple_string() {
        let mut codec = RedisCodec::new();
        let mut buffer = BytesMut::from("+OK\r\n");
        let decoded = codec.decode(&mut buffer);
        assert_eq!(decoded, Ok(Some(SimpleString("OK".to_string()))));
    }

    #[test]
    fn test_parse_simple_error() {
        let mut codec = RedisCodec::new();
        let mut buffer = BytesMut::from("-Error message\r\n");
        let decoded = codec.decode(&mut buffer);
        assert_eq!(decoded, Ok(Some(SimpleError("Error message".to_string()))));
    }

    #[test]
    fn test_parse_correct_integer_with_positive_sign() {
        let mut codec = RedisCodec::new();
        let mut buffer = BytesMut::from(":+100\r\n");
        let decoded = codec.decode(&mut buffer);
        assert_eq!(decoded, Ok(Some(Integer(100))));
    }

    #[test]
    fn test_parse_correct_integer_without_sign() {
        let mut codec = RedisCodec::new();
        let mut buffer = BytesMut::from(":100\r\n");
        let decoded = codec.decode(&mut buffer);
        assert_eq!(decoded, Ok(Some(Integer(100))));
    }

    #[test]
    fn test_parse_zero_integer_correctly() {
        let mut codec = RedisCodec::new();
        let mut buffer = BytesMut::from(":0\r\n");
        let decoded = codec.decode(&mut buffer);
        assert_eq!(decoded, Ok(Some(Integer(0))));
    }

    #[test]
    fn test_parse_correct_integer_negative() {
        let mut codec = RedisCodec::new();
        let mut buffer = BytesMut::from(":-100\r\n");
        let decoded = codec.decode(&mut buffer);
        assert_eq!(decoded, Ok(Some(Integer(-100))));
    }

    #[test]
    fn test_parse_error_on_integer_overflow() {
        let mut codec = RedisCodec::new();
        let mut buffer = BytesMut::from(":+100000000000000000000000000000000000000000000000000\r\n");
        let decoded = codec.decode(&mut buffer);
        assert_eq!(decoded, Err(MalformedInteger));
    }

    #[test]
    fn test_parse_error_on_invalid_integer_format() {
        let mut codec = RedisCodec::new();

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
        let mut codec = RedisCodec::new();
        let mut buffer = BytesMut::from("$5\r\nhello\r\n");
        let decoded = codec.decode(&mut buffer);
        assert_eq!(decoded, Ok(Some(BulkString("hello".to_string()))));
    }

    #[test]
    fn test_parse_well_formed_null_bulk_string() {
        let mut codec = RedisCodec::new();
        let mut buffer = BytesMut::from("$-1\r\n");
        let decoded = codec.decode(&mut buffer);
        assert_eq!(decoded, Ok(Some(Null)));
    }

    #[test]
    fn test_parse_well_formed_empty_bulk_string() {
        let mut codec = RedisCodec::new();
        let mut buffer = BytesMut::from("$0\r\n\r\n");
        let decoded = codec.decode(&mut buffer);
        assert_eq!(decoded, Ok(Some(BulkString("".to_string()))));
    }

    #[test]
    fn test_parse_bad_bulk_string_with_invalid_negative_length() {
        let mut codec = RedisCodec::new();
        let mut buffer = BytesMut::from("$-10\r\n");
        let decoded = codec.decode(&mut buffer);
        assert_eq!(decoded, Err(NegativeLength));
    }

    #[test]
    fn test_parse_bad_bulk_string_with_length_mismatch_more_chars() {
        let mut codec = RedisCodec::new();
        let mut buffer = BytesMut::from("$5\r\nhelloo\r\n");
        let decoded = codec.decode(&mut buffer);
        assert_eq!(decoded, Err(MalformedBulkStringLengthMismatch));
    }

    #[test]
    fn test_parse_bad_bulk_string_with_length_mismatch_fewer_chars() {
        let mut codec = RedisCodec::new();
        let mut buffer = BytesMut::from("$5\r\nhell\r\n");
        let decoded = codec.decode(&mut buffer);
        assert_eq!(decoded, Err(MalformedBulkStringLengthMismatch));
    }

    #[test]
    fn test_parse_empty_array() {
        let mut codec = RedisCodec::new();
        let mut buffer = BytesMut::from("*0\r\n");
        let decoded = codec.decode(&mut buffer);
        assert_eq!(decoded, Ok(Some(Array(vec![]))));
    }

    #[test]
    fn test_parse_array_bulk_string_hello_world() {
        let mut codec = RedisCodec::new();
        let mut buffer = BytesMut::from("*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n");
        let decoded = codec.decode(&mut buffer);
        assert_eq!(decoded, Ok(Some(Array(vec![
            BulkString("hello".to_string()),
            BulkString("world".to_string()),
        ]))));
    }

    #[test]
    fn test_parse_array_list_of_integers() {
        let mut codec = RedisCodec::new();
        let mut buffer = BytesMut::from("*3\r\n:1\r\n:2\r\n:3\r\n");
        let decoded = codec.decode(&mut buffer);
        assert_eq!(decoded, Ok(Some(Array(vec![Integer(1), Integer(2), Integer(3)]))));
    }

    #[test]
    fn test_parse_array_of_mixed_data_types() {
        let mut codec = RedisCodec::new();
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
        let mut codec = RedisCodec::new();
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
        let mut codec = RedisCodec::new();
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
            ("not a number - NaN", ",nan\r\n", Ok(Some(Double(f64::NAN)))),
        ] {
            let mut buffer = BytesMut::from(redis_str);
            let decoded = codec.decode(&mut buffer);
            assert_eq!(decoded, expected, "failed test {name:?}")
        }
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