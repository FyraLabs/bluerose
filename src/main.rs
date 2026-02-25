use color_eyre::{Result, eyre::bail};
use serde::{Deserialize, Serialize};
use std::future::pending;
use std::sync::Arc;
use tokio::process::Command;
use tokio::sync::Mutex;
use tokio_stream::StreamExt;
use zbus::zvariant::{Optional, OwnedValue, Type, Value};
use zbus::{connection, interface};

use crate::bootc::{get_status, UpdateInfo};
use crate::types::host::{Host, ImageStatus};
use crate::types::progress::{DbusEvent, Event};

mod bootc;
mod types;

#[derive(Deserialize, Serialize, Type, PartialEq, Debug, Value, OwnedValue)]
struct Image {
    pub image: String,
    pub digest: String,
    /// A conversion of DateTime<Utc>,
    ///
    /// This is used instead of the raw DateTime type because zbus does not support serializing chrono structs
    pub timestamp: Optional<u64>,
}

impl From<ImageStatus> for Image {
    fn from(status: ImageStatus) -> Self {
        Self {
            image: status.image.image,
            digest: status.image_digest,
            timestamp: Optional::from(status.timestamp.map(|ts| ts.timestamp_millis() as u64)),
        }
    }
}

fn process_event_stream(
    mut event_stream: impl tokio_stream::Stream<Item = Result<Event>> + Unpin + Send + 'static,
    child_handle: Arc<Mutex<Option<tokio::process::Child>>>,
    current_event: Arc<Mutex<Option<DbusEvent>>>,
) -> tokio::task::JoinHandle<()> {
    // Spawn a task to consume the event stream and update the current_event property.
    tokio::spawn(async move {
        while let Some(event) = event_stream.next().await {
            match event {
                Ok(event_raw) => {
                    let event = match DbusEvent::try_from(event_raw) {
                        Ok(e) => e,
                        Err(e) => {
                            eprintln!("Error converting bootc event to DbusEvent: {e:?}");
                            continue;
                        }
                    };

                    // only lock in this scope :3
                    {
                        *current_event.lock().await = Some(event.clone());
                    }
                }
                Err(e) => {
                    eprintln!("Error receiving bootc event: {e}");
                    break;
                }
            }
        }

        if let Some(mut child) = child_handle.lock().await.take() {
            let _ = child.wait().await;
        }

        {
            *current_event.lock().await = None;
        }
    })
}

#[derive(Default)]
struct Bluerose {
    // /// Currently emitted event from bootc --progress-fd, if any. This is not persisted across calls to bootc, so it will be None when no bootc command is currently running.
    pub current_event: Arc<Mutex<Option<DbusEvent>>>,
    pub event_task: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    pub child_handle: Arc<Mutex<Option<tokio::process::Child>>>,
}

#[interface(name = "com.fyralabs.Bluerose1")]
impl Bluerose {
    #[zbus(property)]
    async fn staged_image(&self) -> zbus::fdo::Result<Option<Image>> {
        let host = get_status()
            .await
            .map_err(|e| zbus::fdo::Error::Failed(e.to_string()))?;

        Ok(host
            .status
            .staged
            .map(|b| b.image.map(|b| b.into()))
            .flatten())
    }

    // #[zbus(property)]
    async fn booted_image(&mut self) -> zbus::fdo::Result<Option<Image>> {
        let host = get_status()
            .await
            .map_err(|e| zbus::fdo::Error::Failed(e.to_string()))?;

        Ok(host
            .status
            .booted
            .map(|b| b.image.map(|b| b.into()))
            .flatten())
    }

    // #[zbus(property)]
    async fn rollback_image(&mut self) -> zbus::fdo::Result<Option<Image>> {
        let host = get_status()
            .await
            .map_err(|e| zbus::fdo::Error::Failed(e.to_string()))?;

        Ok(host
            .status
            .rollback
            .map(|b| b.image.map(|b| b.into()))
            .flatten())
    }

    async fn rollback(&mut self, apply: bool) -> zbus::fdo::Result<()> {
        bootc::rollback(apply)
            .await
            .map_err(|e| zbus::fdo::Error::Failed(e.to_string()))
    }
    
    async fn check_update(&mut self) -> zbus::fdo::Result<Option<UpdateInfo>> {
        bootc::check_update()
            .await
            .map_err(|e| zbus::fdo::Error::Failed(e.to_string()))
    }

    async fn apply_usr_overlay(&mut self) -> zbus::fdo::Result<()> {
        bootc::apply_usr_overlay()
            .await
            .map_err(|e| zbus::fdo::Error::Failed(e.to_string()))
    }

    #[zbus(property)]
    fn current_event(&self) -> zbus::fdo::Result<Option<DbusEvent>> {
        let event = self.current_event.blocking_lock().clone();
        Ok(event.clone())
    }

    /// Abort the currently running bootc operation, if any.
    ///
    /// This will kill the managed bootc process
    /// and clear the current_event property, which should cause any clients to stop waiting for progress updates.
    async fn abort(&mut self) -> zbus::fdo::Result<()> {
        if let Some(task) = self.event_task.lock().await.take() {
            task.abort();
        }

        if let Some(mut child) = self.child_handle.lock().await.take() {
            let _ = child.kill().await;
            let _ = child.wait().await;
        }

        let mut current_event = self.current_event.lock().await;
        *current_event = None;

        Ok(())
    }

    async fn upgrade(&mut self) -> zbus::fdo::Result<()> {
        let current_event = self.current_event.clone();
        let (event_stream, child) = bootc::upgrade()
            .await
            .map_err(|e| zbus::fdo::Error::Failed(e.to_string()))?;

        {
            let mut child_handle = self.child_handle.lock().await;
            *child_handle = Some(child);
        }

        let event_task =
            process_event_stream(event_stream, self.child_handle.clone(), current_event);
        let mut handle = self.event_task.lock().await;
        if let Some(old_task) = handle.take() {
            old_task.abort();
        }
        *handle = Some(event_task);

        Ok(())
    }

    async fn switch(&mut self, img_ref: &str) -> zbus::fdo::Result<()> {
        let current_event = self.current_event.clone();
        let (event_stream, child) = bootc::switch(img_ref)
            .await
            .map_err(|e| zbus::fdo::Error::Failed(e.to_string()))?;

        {
            let mut child_handle = self.child_handle.lock().await;
            *child_handle = Some(child);
        }

        let event_task =
            process_event_stream(event_stream, self.child_handle.clone(), current_event);
        let mut handle = self.event_task.lock().await;
        if let Some(old_task) = handle.take() {
            old_task.abort();
        }
        *handle = Some(event_task);

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // upgrade().await?;
    let bluerose = Bluerose::default();
    let _conn = connection::Builder::system()?
        .name("com.fyralabs.Bluerose")?
        .serve_at("/com/fyralabs/Bluerose", bluerose)?
        .build()
        .await?;

    pending::<()>().await;

    Ok(())
}
