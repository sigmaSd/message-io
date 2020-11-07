use std::collections::{HashMap, hash_map::Entry};
use anyhow::{Result, Context};
use std::convert::TryFrom;

type Padding = u32;
const PADDING: usize = std::mem::size_of::<Padding>();

/// Prepare the buffer to encode data.
/// The user should copy the data to the callback buffer in order to encode it.
pub fn encode<C: Fn(&mut Vec<u8>) -> Result<()>>(
    buffer: &mut Vec<u8>,
    encode_callback: C,
) -> Result<()>
{
    let start_point = buffer.len();
    buffer.extend_from_slice(&[0; PADDING]); //Start serializing after PADDING
    let message_point = buffer.len();
    encode_callback(buffer)?;
    assert!(buffer.len() >= message_point, "Encoding must not decrement the buffer length");
    let data_size = Padding::try_from(buffer.len() - message_point)
        .context("data size larger than padding?")?;
    bincode::serialize_into(&mut buffer[start_point..start_point + PADDING], &data_size)
        .context("failed to serialize data size")?;
    Ok(())
}

pub struct Decoder {
    decoded_data: Vec<u8>,
    expected_size: Option<usize>,
}

impl Decoder {
    pub fn new() -> Decoder {
        Decoder { decoded_data: Vec::new(), expected_size: None }
    }

    /// Tries to decode the data without reserve any memory.
    /// The function returns the decoded data and the remaining data or `None`
    /// if more data is necessary to decode it.
    /// If this function returns None, a call to `decode()` is needed.
    pub fn try_fast_decode(data: &[u8]) -> Result<Option<(&[u8], &[u8])>> {
        if data.len() >= PADDING {
            let expected_size = usize::try_from(bincode::deserialize::<Padding>(data)?)?;
            if data[PADDING..].len() >= expected_size {
                return Ok(Some((
                    &data[PADDING..PADDING + expected_size], // decoded data
                    &data[PADDING + expected_size..],        // next undecoded data
                )))
            }
        }
        Ok(None)
    }

    /// Given data, it tries to decode it grouping several data slots if necessary.
    /// The function returns a tuple.
    /// In the first place it returns the decoded data or `None`
    /// if it can not be decoded yet because it needs more data.
    /// In second place it retuns the remaing data.
    pub fn decode<'a>(&mut self, data: &'a [u8]) -> Result<(Option<&[u8]>, &'a [u8])> {
        let next_data = if let Some(expected_size) = self.expected_size {
            let pos = std::cmp::min(expected_size, data.len());
            self.decoded_data.extend_from_slice(&data[..pos]);
            &data[pos..]
        }
        else if data.len() >= PADDING {
            let size_pos = std::cmp::min(PADDING - self.decoded_data.len(), PADDING);
            self.decoded_data.extend_from_slice(&data[..size_pos]);
            let expected_size =
                usize::try_from(bincode::deserialize::<Padding>(&self.decoded_data)?)?;
            self.expected_size = Some(expected_size);

            let data = &data[size_pos..];
            if data.len() < expected_size {
                self.decoded_data.extend_from_slice(data);
                &data[data.len()..]
            }
            else {
                self.decoded_data.extend_from_slice(&data[..expected_size]);
                &data[expected_size..]
            }
        }
        else {
            self.decoded_data.extend_from_slice(data);
            &data[data.len()..]
        };

        if let Some(expected_size) = self.expected_size {
            if self.decoded_data.len() == expected_size + PADDING {
                return Ok((Some(&self.decoded_data[PADDING..]), next_data))
            }
        }

        Ok((None, next_data))
    }
}

pub struct DecodingPool<E> {
    decoders: HashMap<E, Decoder>,
}

impl<E> DecodingPool<E>
where E: std::hash::Hash + Eq
{
    pub fn new() -> DecodingPool<E> {
        DecodingPool { decoders: HashMap::new() }
    }

    pub fn decode_from<C: FnMut(&[u8]) -> Result<()>>(
        &mut self,
        data: &[u8],
        identifier: E,
        mut decode_callback: C,
    ) -> Result<()>
    {
        match self.decoders.entry(identifier) {
            Entry::Vacant(entry) => {
                if let Some(decoder) = Self::fast_decode(data, decode_callback)? {
                    entry.insert(decoder);
                }
            }
            Entry::Occupied(mut entry) => {
                let (decoded_data, next_data) = entry.get_mut().decode(data)?;
                if let Some(decoded_data) = decoded_data {
                    decode_callback(decoded_data)?;
                    match Self::fast_decode(next_data, decode_callback)? {
                        Some(decoder) => entry.insert(decoder),
                        None => entry.remove(),
                    };
                }
            }
        }
        Ok(())
    }

    fn fast_decode<C: FnMut(&[u8]) -> Result<()>>(
        data: &[u8],
        mut decode_callback: C,
    ) -> Result<Option<Decoder>>
    {
        let mut next_data = data;
        loop {
            if let Some((decoded_data, reminder_data)) = Decoder::try_fast_decode(next_data)? {
                decode_callback(decoded_data)?;
                if reminder_data.is_empty() {
                    return Ok(None)
                }
                next_data = reminder_data;
            }
            else {
                let mut decoder = Decoder::new();
                decoder.decode(next_data)?; // It will not be ready with the reminder data. We safe it and wait the next data.
                return Ok(Some(decoder))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MESSAGE_VALUE: u8 = 5;
    const MESSAGE_SIZE: usize = 20; // only works for pair numbers
    const MESSAGE: [u8; MESSAGE_SIZE] = [MESSAGE_VALUE; MESSAGE_SIZE];

    fn encode_message(buffer: &mut Vec<u8>) {
        super::encode(buffer, |slot| {
            slot.extend_from_slice(&MESSAGE);
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn encode() {
        let mut buffer = Vec::new();
        encode_message(&mut buffer);

        assert_eq!(buffer.len(), PADDING + MESSAGE_SIZE);
        let expected_size = bincode::deserialize::<Padding>(&buffer[0..]).unwrap() as usize;
        assert_eq!(expected_size, MESSAGE_SIZE);
        assert_eq!(&buffer[PADDING..], &MESSAGE);
    }

    #[test]
    fn fast_decode_exact() {
        let mut buffer = Vec::new();
        encode_message(&mut buffer);

        let (decoded_data, next_data) = Decoder::try_fast_decode(&buffer).unwrap().unwrap();
        assert_eq!(next_data.len(), 0);
        assert_eq!(decoded_data, &MESSAGE);
    }

    #[test]
    fn decode_exact() {
        let mut buffer = Vec::new();
        encode_message(&mut buffer);

        let mut decoder = Decoder::new();
        let (decoded_data, next_data) = decoder.decode(&buffer).unwrap();
        let decoded_data = decoded_data.unwrap();
        assert_eq!(next_data.len(), 0);
        assert_eq!(decoded_data, &MESSAGE);
    }

    #[test]
    fn fast_decode_exact_multiple() {
        const MESSAGES_NUMBER: usize = 3;

        let mut buffer = Vec::new();
        for _ in 0..MESSAGES_NUMBER {
            encode_message(&mut buffer);
        }

        let mut message_index = 0;
        let mut next_data = &buffer[..];
        loop {
            message_index += 1;
            if let Some((decoded_data, reminder_data)) =
                Decoder::try_fast_decode(next_data).unwrap()
            {
                println!("{}", message_index);
                assert_eq!(
                    reminder_data.len(),
                    (MESSAGE_SIZE + PADDING) * (MESSAGES_NUMBER - message_index)
                );
                assert_eq!(decoded_data, &MESSAGE);
                if reminder_data.len() == 0 {
                    assert_eq!(message_index, MESSAGES_NUMBER);
                    break
                }
                next_data = reminder_data;
            }
        }
    }

    #[test]
    fn decode_in_parts() {
        let mut buffer = Vec::new();
        encode_message(&mut buffer);

        let mut decoder = Decoder::new();
        let (first, second) = buffer.split_at(MESSAGE_SIZE / 2);

        let (decoded_data, next_data) = decoder.decode(&first).unwrap();
        assert!(decoded_data.is_none());
        assert_eq!(next_data.len(), 0);

        let (decoded_data, next_data) = decoder.decode(&second).unwrap();
        let decoded_data = decoded_data.unwrap();
        assert_eq!(next_data.len(), 0);
        assert_eq!(decoded_data, &MESSAGE);
    }

    //TODO: test DecodingPool
}
