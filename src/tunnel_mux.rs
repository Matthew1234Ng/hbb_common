use crate::Stream;
use bytes::BytesMut;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::mpsc;

pub struct TunnelMux {
    rx: mpsc::Receiver<io::Result<BytesMut>>,
    tx: mpsc::UnboundedSender<Vec<u8>>,
    current: Option<BytesMut>,
}

impl TunnelMux {
    pub fn new(stream: Stream) -> Self {
        Self::with_prefill(stream, Vec::new())
    }

    pub fn with_prefill(stream: Stream, prefill: Vec<u8>) -> Self {
        let (tx_read, rx) = mpsc::channel::<io::Result<BytesMut>>(64);
        let (tx, mut rx_write) = mpsc::unbounded_channel::<Vec<u8>>();

        if !prefill.is_empty() {
            let tx_read2 = tx_read.clone();
            tokio::spawn(async move {
                let _ = tx_read2.send(Ok(BytesMut::from(&prefill[..]))).await;
            });
        }

        tokio::spawn(async move {
            TunnelMux::run(stream, tx_read, &mut rx_write).await;
        });

        Self { rx, tx, current: None }
    }

    async fn run(
        mut stream: Stream,
        tx: mpsc::Sender<io::Result<BytesMut>>,
        rx: &mut mpsc::UnboundedReceiver<Vec<u8>>,
    ) {
        let _ = std::fs::create_dir_all("C:\\ProgramData\\digiDesk");
        loop {
            tokio::select! {
                data = stream.next() => {
                    match data {
                        Some(Ok(bytes)) => {
                            let len = bytes.len();
                            let _ = std::fs::OpenOptions::new().create(true).append(true).open("C:\\ProgramData\\digiDesk\\digidesk_debug.log").and_then(|mut f| {
                                use std::io::Write;
                                writeln!(f, "TUNNELMUX_READ: len={}, first_bytes={:02x?}", len, &bytes.as_ref()[..len.min(16)])
                            });
                            if tx.send(Ok(bytes)).await.is_err() { break; }
                        }
                        Some(Err(e)) => {
                            let _ = tx.send(Err(e)).await;
                            break;
                        }
                        None => {
                            let _ = tx.send(Err(io::Error::new(io::ErrorKind::ConnectionAborted, "stream closed"))).await;
                            break;
                        }
                    }
                }
                data = rx.recv() => {
                    match data {
                        Some(bytes) => {
                            let _ = std::fs::OpenOptions::new().create(true).append(true).open("C:\\ProgramData\\digiDesk\\digidesk_debug.log").and_then(|mut f| {
                                use std::io::Write;
                                writeln!(f, "TUNNELMUX_WRITE: len={}", bytes.len())
                            });
                            if stream.send_bytes(bytes.into()).await.is_err() { break; }
                        }
                        None => { break; }
                    }
                }
            }
        }
    }
}

impl AsyncRead for TunnelMux {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if let Some(data) = self.current.as_mut() {
            let avail = data.len();
            let want = buf.remaining();
            let take = avail.min(want);
            buf.put_slice(&data[..take]);
            if take < avail {
                let remaining = data.split_off(take);
                self.current = Some(remaining);
            } else {
                self.current = None;
            }
            return Poll::Ready(Ok(()));
        }

        match Pin::new(&mut self.rx).poll_recv(cx) {
            Poll::Ready(Some(Ok(mut data))) => {
                let avail = data.len();
                let want = buf.remaining();
                let take = avail.min(want);
                buf.put_slice(&data[..take]);
                if take < avail {
                    let _ = data.split_to(take);
                    self.current = Some(data);
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Err(e)),
            Poll::Ready(None) => Poll::Ready(Err(io::Error::new(io::ErrorKind::ConnectionAborted, "mux reader closed"))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncWrite for TunnelMux {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.tx.send(buf.to_vec()) {
            Ok(_) => Poll::Ready(Ok(buf.len())),
            Err(_) => Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, "mux writer closed"))),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}