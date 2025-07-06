use futures_lite::{AsyncRead, AsyncWrite};
use glommio::net::TcpStream;
use hyper::{
    Error,
    body::{Body as HttpBody, Bytes, Frame},
};

use std::{
    io,
    marker::PhantomData,
    pin::Pin,
    slice,
    task::{Context, Poll},
};

pub(crate) struct HyperStream(pub TcpStream);

impl hyper::rt::Write for HyperStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_close(cx)
    }
}

impl hyper::rt::Read for HyperStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<std::io::Result<()>> {
        unsafe {
            let read_slice = {
                let buffer = buf.as_mut();
                buffer.as_mut_ptr().write_bytes(0, buffer.len());
                slice::from_raw_parts_mut(buffer.as_mut_ptr() as *mut u8, buffer.len())
            };
            Pin::new(&mut self.0).poll_read(cx, read_slice).map(|n| {
                if let Ok(n) = n {
                    buf.advance(n);
                }
                Ok(())
            })
        }
    }
}

pub struct ResponseBody {
    // Our ResponseBody type is !Send and !Sync
    _marker: PhantomData<*const ()>,
    data: Option<Bytes>,
}

impl From<&'static str> for ResponseBody {
    fn from(data: &'static str) -> Self {
        ResponseBody {
            _marker: PhantomData,
            data: Some(Bytes::from(data)),
        }
    }
}

impl HttpBody for ResponseBody {
    type Data = Bytes;
    type Error = Error;
    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        Poll::Ready(self.get_mut().data.take().map(|d| Ok(Frame::data(d))))
    }
}
