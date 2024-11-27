use crate::{Connection, Db, Frame, Parse};

use bytes::Bytes;
use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Del {
    /// Name of the key to delete
    key: String,
}

impl Del {
    /// Create a new `Del` command which fetches `key`.
    pub fn new(key: impl ToString) -> Del {
        Del {
            key: key.to_string(),
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Del> {
        let key = parse.next_string()?;

        Ok(Del { key })
    }

    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let response;
        if let Err(s) = db.del(&self.key) {
            response = Frame::Simple(s.to_string());
        } else {
            // Create a success response and write it to `dst`.
            response = Frame::Simple("OK".to_string());
        }
        debug!(?response);

        // Write the response back to the client
        dst.write_frame(&response).await?;

        Ok(())
    }

    /// Converts the command into an equivalent `Frame`.
    ///
    /// This is called by the client when encoding a `Del` command to send to
    /// the server.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("del".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        frame
    }
}
