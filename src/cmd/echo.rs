use crate::{Connection, Frame, Parse, ParseError};
use bytes::Bytes;
use tracing::{debug, instrument};

/// Returns a copy of the argument as a bulk.
///
/// This command is often used to test if a connection
/// is still alive, or to measure latency.
#[derive(Debug, Default)]
pub struct Echo {
    /// message to be returned
    msg: Bytes,
}

impl Echo {
    /// Create a new `Echo` command with `msg`.
    pub fn new(msg: Bytes) -> Echo {
        Echo { msg }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Echo> {
        match parse.next_bytes() {
            Ok(msg) => Ok(Echo::new(msg)),
            Err(ParseError::EndOfStream) => Ok(Echo::default()),
            Err(e) => Err(e.into()),
        }
    }

    /// Apply the `Echo` command and return the message.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received command.
    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = Frame::Bulk(self.msg);

        debug!(?response);

        // Write the response back to the client
        dst.write_frame(&response).await?;

        Ok(())
    }

    /// Converts the command into an equivalent `Frame`.
    ///
    /// This is called by the client when encoding a `Echo` command to send
    /// to the server.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("echo".as_bytes()));
        frame.push_bulk(self.msg);
        frame
    }
}
