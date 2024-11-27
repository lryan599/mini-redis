use crate::cmd::{Parse, ParseError};
use crate::{Connection, Db, Frame};

use bytes::Bytes;
use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Incr {
    /// the lookup key
    key: String,
}

impl Incr {
    pub fn new(key: impl ToString) -> Incr {
        Incr {
            key: key.to_string(),
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Incr> {
        // The `GET` string has already been consumed. The next value is the
        // name of the key to get. If the next value is not a string or the
        // input is fully consumed, then an error is returned.
        let key = parse.next_string()?;

        Ok(Incr { key })
    }

    /// Apply the `Incr` command to the specified `Db` instance.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received command.
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        // Incr the value in the shared database state.
        let response;
        if let Some(v) = db.get(&self.key) {
            // 解析v是否是整数
            use atoi::atoi;
            if let Some(int) = atoi::<i64>(&v) {
                let b = Bytes::copy_from_slice((int + 1).to_string().as_bytes());
                if let Err(s) = db.set(self.key, b, None, None) {
                    response = Frame::Simple(s.to_string());
                } else {
                    response = Frame::Integer((int + 1) as u64);
                }
            } else {
                response = Frame::Simple("value is not an integer".into());
            }
        } else {
            let b = Bytes::from_static(b"0");
            if let Err(s) = db.set(self.key, b, None, None) {
                response = Frame::Simple(s.to_string());
            } else {
                response = Frame::Integer(0);
            }
        }
        debug!(?response);
        dst.write_frame(&response).await?;
        Ok(())
    }

    /// Converts the command into an equivalent `Frame`.
    ///
    /// This is called by the client when encoding a `Incr` command to send to
    /// the server.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("incr".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        frame
    }
}
