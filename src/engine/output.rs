//! Output ownership stays accessible while polling or closing is cancelled.

use std::future::Future;
use std::pin::Pin;
use std::task::{
    Context,
    Poll,
};

use futures_util::Stream;
use futures_util::future::poll_fn;

use super::cleanup::AsyncCleanupGuard;
use super::merge::ValueStream;
use crate::SorterError;

type Closing = Pin<Box<dyn Future<Output = (AsyncCleanupGuard, Result<(), SorterError>)> + Send>>;

/// Sorted values with explicit, awaited resource cleanup.
///
/// Exhaustion cleans up before reporting EOF. `close` and `abort` release an
/// unread suffix without draining it. Dropping uses best-effort async cleanup;
/// callers requiring a completion guarantee must await either method.
pub struct SortOutput<V> {
    rows: Option<ValueStream<V>>,
    scratch: Option<AsyncCleanupGuard>,
    closing: Option<Closing>,
    hold: Option<Box<dyn Send>>,
}

impl<V> SortOutput<V> {
    pub(crate) fn new(
        rows: ValueStream<V>,
        scratch: AsyncCleanupGuard,
        hold: Option<Box<dyn Send>>,
    ) -> Self {
        Self {
            rows: Some(rows),
            scratch: Some(scratch),
            closing: None,
            hold,
        }
    }

    /// Discard unread values and await original I/O and scratch removal.
    /// A cancelled close can be resumed by calling this method again.
    ///
    /// # Errors
    /// Returns scratch-removal failures; the original owner remains retryable.
    pub async fn close(&mut self) -> Result<(), SorterError> {
        self.rows = None;
        poll_fn(|cx| self.poll_close(cx)).await
    }

    /// Abort output consumption and await the same cleanup as [`Self::close`].
    ///
    /// # Errors
    /// Returns scratch-removal failures; cleanup can be retried.
    pub async fn abort(&mut self) -> Result<(), SorterError> {
        self.close().await
    }

    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), SorterError>> {
        if self.closing.is_none() {
            let Some(mut scratch) = self.scratch.take() else {
                self.hold = None;
                return Poll::Ready(Ok(()));
            };
            self.closing = Some(Box::pin(async move {
                let result = scratch.cleanup().await;
                (scratch, result)
            }));
        }
        let (scratch, result) = std::task::ready!(
            self.closing
                .as_mut()
                .expect("cleanup exists")
                .as_mut()
                .poll(cx)
        );
        self.closing = None;
        if result.is_err() {
            self.scratch = Some(scratch);
        } else {
            self.hold = None;
        }
        Poll::Ready(result)
    }
}

impl<V> Stream for SortOutput<V> {
    type Item = Result<V, SorterError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(rows) = &mut this.rows {
            match std::task::ready!(rows.as_mut().poll_next(cx)) {
                Some(item) => return Poll::Ready(Some(item)),
                None => this.rows = None,
            }
        }
        match std::task::ready!(this.poll_close(cx)) {
            Ok(()) => Poll::Ready(None),
            Err(error) => Poll::Ready(Some(Err(error))),
        }
    }
}
