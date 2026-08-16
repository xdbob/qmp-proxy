use bytes::{Buf, BytesMut};
use serde_json::Value;
use thiserror::Error;
use tokio_util::codec::Decoder;

/// Decoder for the on-wire QMP protocol as defined in
/// `docs/interop/qmp-spec.rst`
/// Limitation: The decoder does not support the single quote JSON extension
/// supported by the vanilla QMP server.
pub struct QmpDecoder {
    max_size: Option<usize>,
}

impl QmpDecoder {
    pub fn new() -> Self {
        Self { max_size: None }
    }

    pub fn new_bounded(max_size: usize) -> Self {
        Self {
            max_size: Some(max_size),
        }
    }
}

#[derive(Debug, Error)]
pub enum QmpDecoderError {
    #[error("invalid json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json object exceeds max size of {0} bytes")]
    TooLarge(usize),
}

impl Decoder for QmpDecoder {
    type Item = Value;
    type Error = QmpDecoderError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Value>, Self::Error> {
        if let Some(max_size) = self.max_size
            && src.len() > max_size
        {
            return Err(QmpDecoderError::TooLarge(max_size));
        }

        let start = match src.iter().position(|b| !b.is_ascii_whitespace()) {
            Some(s) => s,
            None => {
                src.clear();
                return Ok(None);
            }
        };
        src.advance(start);

        if src.is_empty() {
            return Ok(None);
        }

        let mut de = serde_json::Deserializer::from_slice(&src[..]).into_iter::<Value>();

        match de.next() {
            Some(Ok(value)) => {
                let consumed = de.byte_offset();
                src.advance(consumed);
                Ok(Some(value))
            }
            Some(Err(e)) if e.is_eof() => Ok(None),
            Some(Err(e)) => Err(QmpDecoderError::Json(e)),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BufMut;
    use serde_json::Map;

    fn setup(raw_data: &[u8]) -> (BytesMut, QmpDecoder) {
        let mut buf = BytesMut::new();
        buf.put(raw_data);
        (buf, QmpDecoder::new())
    }

    #[test]
    fn test_single_message() {
        let (mut buf, mut decoder) = setup(br#"{"hello": "world"}"#);
        let val = decoder
            .decode_eof(&mut buf)
            .expect("EOF")
            .expect("Decode error");

        let mut map = Map::new();
        map.insert("hello".to_string(), Value::String("world".to_string()));
        let obj = Value::Object(map);
        assert!(val == obj);
    }

    #[test]
    fn test_multiple_messages() {
        let (mut buf, mut decoder) = setup(
            br#"{"hello": "world"}
            {"hello": "coucou"}"#,
        );
        let _ = decoder
            .decode_eof(&mut buf)
            .expect("EOF")
            .expect("Decode error");
        let val = decoder
            .decode_eof(&mut buf)
            .expect("EOF")
            .expect("Decode error");

        let mut map = Map::new();
        map.insert("hello".to_string(), Value::String("coucou".to_string()));
        let obj = Value::Object(map);
        assert!(val == obj);
    }

    #[test]
    fn test_invalid_json() {
        let (mut buf, mut decoder) = setup(br#"{"hello": "world"}blblblbl"#);
        let _ = decoder
            .decode_eof(&mut buf)
            .expect("EOF")
            .expect("Decode error");
        let val = decoder.decode_eof(&mut buf);
        assert!(matches!(val, Err(QmpDecoderError::Json(_))));
    }

    #[test]
    fn test_early_eof() {
        let (mut buf, mut decoder) = setup(br#"{"hello": "wor"#);
        let val = decoder.decode_eof(&mut buf);
        assert!(matches!(val, Err(QmpDecoderError::Io(_))));
    }

    #[test]
    fn test_size() {
        let mut buf = BytesMut::new();
        buf.put(&br#"{"hello": "world"}"#[..]);
        let mut decoder = QmpDecoder::new_bounded(4);
        let val = decoder.decode_eof(&mut buf);
        print!("{val:?}");
        assert!(matches!(val, Err(QmpDecoderError::TooLarge(4))));
    }

    #[test]
    fn test_partial_then_complete() {
        let mut buf = BytesMut::new();
        buf.put(&br#"{"hello": "wo"#[..]);
        let mut decoder = QmpDecoder::new();
        assert!(decoder.decode(&mut buf).unwrap().is_none());
        buf.put(&br#"rld"}"#[..]);
        let val = decoder.decode(&mut buf).unwrap().unwrap();
        assert_eq!(val["hello"], "world");
    }

    #[test]
    fn test_ignores_leading_whitespace() {
        let (mut buf, mut decoder) = setup(b"\r\n\t{\"a\": 1}");
        let val = decoder
            .decode(&mut buf)
            .expect("EOF")
            .expect("Decode error");
        assert_eq!(val, serde_json::json!({"a": 1}));
    }
}
