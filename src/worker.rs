use std::collections::HashSet;
use std::time::Duration;

use anyhow::Context;
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use crate::exchange::{BookMessage, ExchangeAdapter};

#[derive(Debug)]
pub enum WorkerEvent {
    Connection {
        replica: usize,
        shard: usize,
        connected: bool,
        reason: Option<String>,
    },
    Feed {
        replica: usize,
        shard: usize,
        received_at: DateTime<Utc>,
        received_instant: Instant,
        message: BookMessage,
    },
}

#[derive(Debug)]
pub enum WorkerCommand {
    Replace(HashSet<String>),
    Refresh(HashSet<String>),
    Stop,
}

pub struct WorkerHandle {
    pub command: mpsc::Sender<WorkerCommand>,
    task: JoinHandle<()>,
}

impl WorkerHandle {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn<A: ExchangeAdapter>(
        replica: usize,
        shard: usize,
        adapter: A,
        symbols: HashSet<String>,
        events: mpsc::Sender<WorkerEvent>,
        socket_stale: Duration,
        reconnect_interval: Duration,
    ) -> Self {
        let (command, commands) = mpsc::channel(32);
        let task = tokio::spawn(run_worker(
            replica,
            shard,
            adapter,
            symbols,
            events,
            commands,
            socket_stale,
            reconnect_interval,
        ));
        Self { command, task }
    }

    pub async fn stop(self) {
        let _ = self.command.send(WorkerCommand::Stop).await;
        let _ = self.task.await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_worker<A: ExchangeAdapter>(
    replica: usize,
    shard: usize,
    adapter: A,
    mut desired: HashSet<String>,
    events: mpsc::Sender<WorkerEvent>,
    mut commands: mpsc::Receiver<WorkerCommand>,
    socket_stale: Duration,
    reconnect_interval: Duration,
) {
    let mut backoff = Duration::from_millis(250);
    loop {
        if desired.is_empty() {
            match commands.recv().await {
                Some(WorkerCommand::Replace(symbols)) => desired = symbols,
                Some(WorkerCommand::Stop) | None => return,
                Some(WorkerCommand::Refresh(_)) => continue,
            }
        }
        let result = run_session(
            replica,
            shard,
            &adapter,
            &mut desired,
            &events,
            &mut commands,
            socket_stale,
            reconnect_interval,
        )
        .await;
        match result {
            Ok(SessionOutcome::Stop) => {
                let _ = events
                    .send(WorkerEvent::Connection {
                        replica,
                        shard,
                        connected: false,
                        reason: None,
                    })
                    .await;
                return;
            }
            Ok(SessionOutcome::Reconnect) => {
                let _ = events
                    .send(WorkerEvent::Connection {
                        replica,
                        shard,
                        connected: false,
                        reason: Some("scheduled or requested reconnect".to_owned()),
                    })
                    .await;
                backoff = Duration::from_millis(250);
            }
            Err(error) => {
                let _ = events
                    .send(WorkerEvent::Connection {
                        replica,
                        shard,
                        connected: false,
                        reason: Some(format!("{error:#}")),
                    })
                    .await;
            }
        }

        let sleep = tokio::time::sleep(backoff);
        tokio::pin!(sleep);
        loop {
            tokio::select! {
                _ = &mut sleep => break,
                command = commands.recv() => match command {
                    Some(WorkerCommand::Replace(symbols)) => desired = symbols,
                    Some(WorkerCommand::Stop) | None => return,
                    Some(WorkerCommand::Refresh(_)) => {}
                }
            }
        }
        backoff = (backoff * 2).min(Duration::from_secs(15));
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_session<A: ExchangeAdapter>(
    replica: usize,
    shard: usize,
    adapter: &A,
    desired: &mut HashSet<String>,
    events: &mpsc::Sender<WorkerEvent>,
    commands: &mut mpsc::Receiver<WorkerCommand>,
    socket_stale: Duration,
    reconnect_interval: Duration,
) -> anyhow::Result<SessionOutcome> {
    let (mut websocket, _) = connect_async(adapter.websocket_request()?)
        .await
        .context("connect WebSocket")?;
    let mut subscribed = desired.clone();
    websocket
        .send(adapter.initial_subscription(&subscribed))
        .await?;
    events
        .send(WorkerEvent::Connection {
            replica,
            shard,
            connected: true,
            reason: None,
        })
        .await?;

    let mut heartbeat = tokio::time::interval(Duration::from_secs(10));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    heartbeat.tick().await;
    let mut health = tokio::time::interval(Duration::from_secs(1));
    health.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let rotation = tokio::time::sleep(reconnect_interval);
    tokio::pin!(rotation);
    let mut last_frame = Instant::now();

    loop {
        tokio::select! {
            frame = websocket.next() => {
                let frame = frame.context("WebSocket closed")??;
                let received_at = Utc::now();
                let received_instant = Instant::now();
                last_frame = received_instant;
                if let Message::Text(text) = frame {
                    for message in adapter.decode_text(text.as_ref())? {
                        events.send(WorkerEvent::Feed {
                            replica,
                            shard,
                            received_at,
                            received_instant,
                            message,
                        }).await?;
                    }
                }
            }
            command = commands.recv() => match command {
                Some(WorkerCommand::Replace(symbols)) => {
                    let removed: HashSet<_> = subscribed.difference(&symbols).cloned().collect();
                    let added: HashSet<_> = symbols.difference(&subscribed).cloned().collect();
                    send_if_nonempty(&mut websocket, &removed, adapter.unsubscribe(&removed)).await?;
                    send_if_nonempty(&mut websocket, &added, adapter.subscribe(&added)).await?;
                    subscribed = symbols.clone();
                    *desired = symbols;
                    if subscribed.is_empty() {
                        let _ = websocket.send(Message::Close(None)).await;
                        return Ok(SessionOutcome::Reconnect);
                    }
                }
                Some(WorkerCommand::Refresh(symbols)) => {
                    let selected: HashSet<_> = symbols.intersection(&subscribed).cloned().collect();
                    send_if_nonempty(&mut websocket, &selected, adapter.unsubscribe(&selected)).await?;
                    send_if_nonempty(&mut websocket, &selected, adapter.subscribe(&selected)).await?;
                }
                Some(WorkerCommand::Stop) | None => {
                    let _ = websocket.send(Message::Close(None)).await;
                    return Ok(SessionOutcome::Stop);
                }
            },
            _ = heartbeat.tick() => {
                websocket.send(adapter.heartbeat()).await?;
            }
            _ = health.tick() => {
                anyhow::ensure!(last_frame.elapsed() <= socket_stale, "socket stale");
            }
            _ = &mut rotation => {
                let _ = websocket.send(Message::Close(None)).await;
                return Ok(SessionOutcome::Reconnect);
            },
        }
    }
}

async fn send_if_nonempty<S>(
    websocket: &mut tokio_tungstenite::WebSocketStream<S>,
    symbols: &HashSet<String>,
    message: Message,
) -> anyhow::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    if !symbols.is_empty() {
        websocket.send(message).await?;
    }
    Ok(())
}

enum SessionOutcome {
    Reconnect,
    Stop,
}
