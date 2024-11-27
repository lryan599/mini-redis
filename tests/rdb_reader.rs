use bytes::*;
use core::str;
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::num::Wrapping;
use std::{io, u8, vec};

const MAGIC_STRING: [u8; 5] = *b"REDIS";
const VERSION: [u8; 4] = *b"0009";

enum OpCode {
    EOF = 0xff,
    SELECTDB = 0xfe,
    EXPIRETIME = 0xfd,
    EXPIRETIMEMS = 0xfc,
    RESIZEDB = 0xfb,
    AUX = 0xfa,
    KV = 0xf9,
}

impl OpCode {
    fn from_u8(value: u8) -> OpCode {
        match value {
            0xff => OpCode::EOF,
            0xfe => OpCode::SELECTDB,
            0xfd => OpCode::EXPIRETIME,
            0xfc => OpCode::EXPIRETIMEMS,
            0xfb => OpCode::RESIZEDB,
            0xfa => OpCode::AUX,
            _ => OpCode::KV,
        }
    }
}

#[derive(Debug)]
enum Decoding {
    STRING(String),
    I8(i8),
    I16(i16),
    I32(i32),
    ERROR,
}
enum EncodingType {
    STRING,
    I8,
    I16,
    I32,
    ERROR,
}

struct RDB {
    // 0x5245444953, "redis"
    magic_string: [u8; 5],
    // 读时不检查RDB版本，但写为0x30303039
    version: [u8; 4],
    // metadata最多由三个FA组成
    metadata: Bytes,
    // 只考虑一个数据库，0xFE00
    db_selector: [u8; 2],
    // 以FB开头，后接两个length-int
    resize_db: Bytes,
    // 以FD开头，后接一个$unsigned int代表时间戳，后面是$value-type，$string-encoded-key和$encoded-value
    kv_expiry: Bytes,
    // 以FC开头，后接一个$unsigned long代表时间戳，后面是$value-type，$string-encoded-key和$encoded-value
    kv_expiry_ms: Bytes,
    // 没有过期限制的KV，由$value-type，$string-encoded-key和$encoded-value组成
    kv: Bytes,
}

impl RDB {
    fn new() -> RDB {
        RDB {
            magic_string: MAGIC_STRING,
            version: VERSION,
            metadata: Bytes::new(),
            db_selector: [0xfe, 0],
            resize_db: Bytes::new(),
            kv_expiry: Bytes::new(),
            kv_expiry_ms: Bytes::new(),
            kv: Bytes::new(),
        }
    }

    fn store_kv(key: Decoding, value: Decoding) {
        match (key, value) {
            (Decoding::ERROR, _) => todo!(),
            (_, Decoding::ERROR) => todo!(),
            (k, v) => {
                dbg!((k, v));
            }
        }
    }

    fn get_value(i: &mut usize, v: &Vec<u8>) -> Decoding {
        let (offset, length, t) = RDB::parse_string_encoded_key(&v[*i..]);
        *i += offset;
        let b = &v[*i..*i + length];
        match t {
            EncodingType::STRING => {
                let s = str::from_utf8(b).unwrap();
                *i += length;
                Decoding::STRING(s.to_string())
            }
            EncodingType::I8 => {
                let v = b[0] as i8;
                *i += length;
                Decoding::I8(v)
            }
            EncodingType::I16 => {
                let mut v = Wrapping(b[1] as i16);
                v = (v << 8) + Wrapping(b[0] as i16);
                *i += length;
                Decoding::I16(v.0)
            }
            EncodingType::I32 => {
                let mut v = Wrapping(b[3] as i32);
                v = (v << 8) + Wrapping(b[2] as i32);
                v = (v << 8) + Wrapping(b[1] as i32);
                v = (v << 8) + Wrapping(b[0] as i32);
                *i += length;
                Decoding::I32(v.0)
            }
            EncodingType::ERROR => Decoding::ERROR,
        }
    }

    fn parse_string_encoded_key(v: &[u8]) -> (usize, usize, EncodingType) {
        let m = v[0];
        match m >> 6 {
            0 => {
                let len = (m & 0x3f) as usize;
                return (1, len, EncodingType::STRING);
            }
            1 => {
                let mut len = (m & 0x3f) as usize;
                len = len << 8;
                len = len + v[1] as usize;
                return (2, len, EncodingType::STRING);
            }
            2 => {
                let mut len: usize = 0;
                len = (len + (v[1] as usize)) << 8;
                len = (len + (v[2] as usize)) << 8;
                len = (len + (v[3] as usize)) << 8;
                len = len + v[4] as usize;
                return (5, len, EncodingType::STRING);
            }
            3 => {
                // 这里需要将它识别成有符号整数
                let mode = m & 0x3f;
                match mode {
                    0 => {
                        return (1, 1, EncodingType::I8);
                    }
                    1 => {
                        return (1, 2, EncodingType::I16);
                    }
                    2 => {
                        return (1, 4, EncodingType::I32);
                    }
                    // C3情况下是LZF压缩的字符串，暂不实现
                    3 => {
                        unimplemented!("LZF Compressed String, unimplemented!")
                    }
                    _ => {
                        return (0, 0, EncodingType::ERROR);
                    }
                }
            }
            _ => {
                return (0, 0, EncodingType::ERROR);
            }
        }
    }

    fn read_rdb(p: &str) -> io::Result<RDB> {
        let r = RDB::new();
        let mut file = File::open(p)?;
        let mut buffer = Vec::new();
        // 直接读完整个文件
        file.read_to_end(&mut buffer)?;
        if buffer[0..5] != MAGIC_STRING {
            dbg!(&buffer[0..5]);
            dbg!(MAGIC_STRING);
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "The rdb file is damaged",
            ));
        }
        if buffer[5..9] != VERSION {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "The version of the rdb file does not match",
            ));
        }

        let mut i: usize = 9;
        while buffer[i] != OpCode::EOF as u8 {
            let op = buffer[i];
            match OpCode::from_u8(op) {
                // 当前不检查CHECK_SUM是否正确
                OpCode::EOF => {
                    break;
                }
                OpCode::SELECTDB => {
                    i += 1;
                    if buffer[i] == 0 {
                        i += 1;
                    } else {
                        unimplemented!("Only support one database currently")
                    }
                }
                OpCode::EXPIRETIME => {
                    i += 1;
                    let mut seconds: u32 = buffer[i + 3] as u32;
                    seconds = (seconds << 8) + buffer[i + 2] as u32;
                    seconds = (seconds << 8) + buffer[i + 1] as u32;
                    seconds = (seconds << 8) + buffer[i] as u32;
                    println!("seconds timestamp: {seconds}");
                    i += 4;
                    let key = RDB::get_value(&mut i, &buffer);
                    let value = RDB::get_value(&mut i, &buffer);
                    RDB::store_kv(key, value);
                }
                OpCode::EXPIRETIMEMS => {
                    i += 1;
                    let mut ms: u64 = buffer[i + 7] as u64;
                    ms = (ms << 8) + buffer[i + 6] as u64;
                    ms = (ms << 8) + buffer[i + 5] as u64;
                    ms = (ms << 8) + buffer[i + 4] as u64;
                    ms = (ms << 8) + buffer[i + 3] as u64;
                    ms = (ms << 8) + buffer[i + 2] as u64;
                    ms = (ms << 8) + buffer[i + 1] as u64;
                    ms = (ms << 8) + buffer[i] as u64;
                    println!("ms timestamp: {ms}");
                    i += 8;
                    let key = RDB::get_value(&mut i, &buffer);
                    let value = RDB::get_value(&mut i, &buffer);
                    RDB::store_kv(key, value);
                }
                OpCode::RESIZEDB => {
                    i += 1;
                    let (offset1, length1, _) = RDB::parse_string_encoded_key(&buffer[i..]);
                    println!("The size of HashTable {length1}");
                    i += offset1;
                    let (offset2, length2, _) = RDB::parse_string_encoded_key(&buffer[i..]);
                    println!("The size of Expire HashTable {length2}");
                    i += offset2;
                }
                OpCode::AUX => {
                    i += 1;
                    let key = RDB::get_value(&mut i, &buffer);
                    let value = RDB::get_value(&mut i, &buffer);
                    RDB::store_kv(key, value);
                }
                OpCode::KV => {
                    let value_type = buffer[i];
                    match value_type {
                        0 => {
                            i += 1;
                            let key = RDB::get_value(&mut i, &buffer);
                            let value = RDB::get_value(&mut i, &buffer);
                            RDB::store_kv(key, value);
                        }
                        _ => {
                            unimplemented!("only supported string value currently")
                        }
                    }
                }
            }
        }

        Ok(r)
    }
}

#[cfg(test)]
mod tests {
    // 注意这个惯用法：在 tests 模块中，从外部作用域导入所有名字。
    use super::*;

    #[test]
    fn t1() {
        let _ = RDB::read_rdb("dump.rdb").unwrap();
    }

    #[test]
    fn t2() {
        use std::num::Wrapping;
        let big = Wrapping(std::u32::MAX);
        let sum = big + Wrapping(2_u32);
        let in8: i8 = 254_u8 as i8;
        let in32: i32 = 254_u8 as i32;
        println!("{}, {in8}, {in32}", sum.0);
    }
}
