// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::{Stream, StreamExt};
use mz_pgwire::codec;
use mz_pgwire::message::FrontendStartupMessage;
use tokio::io::{self};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::unbounded_channel;
use tokio_stream::wrappers::TcpListenerStream;

use mz_adapter::command::{Command, Response};
use mz_adapter::{Client, StartupMessage, StartupResponse};
use mz_ore::task;

const MSG: &str = "https://www.youtube.com/watch?v=dQw4w9WgXcQ";

#[tokio::main]
async fn main() {
    let sql_listener = TcpListener::bind("0.0.0.0:6875").await.unwrap();

    let (tx, mut rx) = unbounded_channel();
    let adapter_client = Client::new(tx);

    task::spawn(|| "pgwire_server", {
        let pgwire_server = Server::new(adapter_client);

        async move {
            let mut incoming = TcpListenerStream::new(sql_listener);
            pgwire_server.serve(incoming.by_ref()).await;
        }
    });

    while let Some(cmd) = rx.recv().await {
        match cmd {
            Command::Startup { session, tx, .. } => {
                tx.send(Response {
                    result: Ok(StartupResponse {
                        secret_key: 0,
                        messages: vec![StartupMessage::Notice(MSG)],
                    }),
                    session,
                })
                .unwrap();
            }
            _ => {}
        }
    }
}

/// A server that communicates with clients via the pgwire protocol.
pub struct Server {
    adapter_client: mz_adapter::Client,
}

impl Server {
    /// Constructs a new server.
    pub fn new(adapter_client: Client) -> Server {
        Server {
            adapter_client: config.adapter_client,
        }
    }

    pub async fn handle_connection<A>(&self, conn: A) -> Result<(), anyhow::Error>
    where
        A: AsyncRead + AsyncWrite + AsyncReady + Send + Sync + Unpin + fmt::Debug + 'static,
    {
        let mut adapter_client = self.adapter_client.new_conn()?;
        let conn_id = adapter_client.conn_id();
        let mut conn = Conn::Unencrypted(conn);
        loop {
            let message = codec::decode_startup(&mut conn).await?;

            conn = match message {
                // Clients sometimes hang up during the startup sequence, e.g.
                // because they receive an unacceptable response to an
                // `SslRequest`. This is considered a graceful termination.
                None => return Ok(()),

                Some(FrontendStartupMessage::Startup { version, params }) => {
                    let mut conn = FramedConn::new(conn_id, conn);
                    protocol::run(protocol::RunParams {
                        tls_mode: self.tls.as_ref().map(|tls| tls.mode),
                        adapter_client,
                        conn: &mut conn,
                        version,
                        params,
                        frontegg: self.frontegg.as_ref(),
                    })
                    .await?;
                    conn.flush().await?;
                    return Ok(());
                }

                Some(FrontendStartupMessage::CancelRequest {
                    conn_id,
                    secret_key,
                }) => {
                    adapter_client.cancel_request(conn_id, secret_key).await;
                    // For security, the client is not told whether the cancel
                    // request succeeds or fails.
                    return Ok(());
                }

                Some(FrontendStartupMessage::SslRequest) => match (conn, &self.tls) {
                    (Conn::Unencrypted(mut conn), Some(tls)) => {
                        conn.write_all(&[ACCEPT_SSL_ENCRYPTION]).await?;
                        let mut ssl_stream = SslStream::new(Ssl::new(&tls.context)?, conn)?;
                        if let Err(e) = Pin::new(&mut ssl_stream).accept().await {
                            let _ = ssl_stream.get_mut().shutdown().await;
                            return Err(e.into());
                        }
                        Conn::Ssl(ssl_stream)
                    }
                    (mut conn, _) => {
                        conn.write_all(&[REJECT_ENCRYPTION]).await?;
                        conn
                    }
                },

                Some(FrontendStartupMessage::GssEncRequest) => {
                    conn.write_all(&[REJECT_ENCRYPTION]).await?;
                    conn
                }
            }
        }
    }
}

#[derive(Debug)]
pub enum Conn<A> {
    Unencrypted(A),
    Ssl(SslStream<A>),
}

impl<A> AsyncRead for Conn<A>
where
    A: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut ReadBuf,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Conn::Unencrypted(inner) => Pin::new(inner).poll_read(cx, buf),
            Conn::Ssl(inner) => Pin::new(inner).poll_read(cx, buf),
        }
    }
}

impl<A> AsyncWrite for Conn<A>
where
    A: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Conn::Unencrypted(inner) => Pin::new(inner).poll_write(cx, buf),
            Conn::Ssl(inner) => Pin::new(inner).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Conn::Unencrypted(inner) => Pin::new(inner).poll_flush(cx),
            Conn::Ssl(inner) => Pin::new(inner).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Conn::Unencrypted(inner) => Pin::new(inner).poll_shutdown(cx),
            Conn::Ssl(inner) => Pin::new(inner).poll_shutdown(cx),
        }
    }
}

#[async_trait]
impl<A> AsyncReady for Conn<A>
where
    A: AsyncRead + AsyncWrite + AsyncReady + Sync + Unpin,
{
    async fn ready(&self, interest: Interest) -> io::Result<Ready> {
        match self {
            Conn::Unencrypted(inner) => inner.ready(interest).await,
            Conn::Ssl(inner) => inner.ready(interest).await,
        }
    }
}

/// A connection handler manages an incoming network connection.
#[async_trait]
pub trait ConnectionHandler {
    /// Returns the name of the connection handler for use in e.g. log messages.
    fn name(&self) -> &str;

    /// Serves incoming TCP traffic from `listener`.
    async fn serve<S>(self, mut incoming: S)
    where
        S: Stream<Item = io::Result<TcpStream>> + Unpin + Send,
        Self: Sized + Sync + 'static,
    {
        let self_ref = Arc::new(self);
        while let Some(conn) = incoming.next().await {
            let conn = match conn {
                Ok(conn) => conn,
                Err(err) => {
                    println!("error accepting connection: {}", err);
                    continue;
                }
            };
            // Set TCP_NODELAY to disable tinygram prevention (Nagle's
            // algorithm), which forces a 40ms delay between each query
            // on linux. According to John Nagle [0], the true problem
            // is delayed acks, but disabling those is a receive-side
            // operation (TCP_QUICKACK), and we can't always control the
            // client. PostgreSQL sets TCP_NODELAY on both sides of its
            // sockets, so it seems sane to just do the same.
            //
            // If set_nodelay fails, it's a programming error, so panic.
            //
            // [0]: https://news.ycombinator.com/item?id=10608356
            conn.set_nodelay(true).expect("set_nodelay failed");
            task::spawn(
                || "mux_serve",
                handle_connection(Arc::clone(&self_ref), conn),
            );
        }
    }

    /// Handles the connection.
    async fn handle_connection(&self, conn: TcpStream) -> Result<(), anyhow::Error>;
}

async fn handle_connection<S: ConnectionHandler>(server_ref: Arc<S>, conn: TcpStream) {
    if let Err(e) = server_ref.handle_connection(conn).await {
        println!(
            "error handling connection in {}: {:#}",
            server_ref.name(),
            e
        );
    }
}

#[async_trait]
impl ConnectionHandler for mz_pgwire::Server {
    fn name(&self) -> &str {
        "pgwire server"
    }

    async fn handle_connection(&self, conn: TcpStream) -> Result<(), anyhow::Error> {
        // Using fully-qualified syntax means we won't accidentally call
        // ourselves (i.e., silently infinitely recurse) if the name or type of
        // `mz_pgwire::Server::handle_connection` changes.
        mz_pgwire::Server::handle_connection(self, conn).await
    }
}
