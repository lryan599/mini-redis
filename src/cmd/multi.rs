use crate::{Connection, Frame, Parse, ParseError};
use bytes::Bytes;
use tracing::{debug, instrument};

/// Returns a copy of the argument as a bulk.
///
/// This command is often used to test if a connection
/// is still alive, or to measure latency.
#[derive(Debug, Default)]
pub struct Multi {}

impl Multi {
    pub fn new() -> Multi {
        Multi {}
    }

    pub(crate) fn parse_frames(_parse: &mut Parse) -> crate::Result<Multi> {
        Ok(Multi::new())
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = Frame::Simple("OK".to_string());
        debug!(?response);
        // Write the response back to the client
        dst.write_frame(&response).await?;
        Ok(())
    }

    /// Converts the command into an equivalent `Frame`.
    ///
    /// This is called by the client when encoding a `Multi` command to send
    /// to the server.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("multi".as_bytes()));
        frame
    }
}
