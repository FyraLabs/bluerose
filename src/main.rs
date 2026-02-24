use color_eyre::{Result, eyre::bail};
use serde::{Deserialize, Serialize};
use std::future::pending;
use tokio::process::Command;
use zbus::zvariant::{Optional, OwnedValue, Type, Value};
use zbus::{connection, interface};

use crate::types::host::{Host, ImageStatus};

mod bootc;
mod types;

async fn get_status() -> Result<Host> {
    let output = Command::new("bootc")
        .arg("status")
        .arg("--format")
        .arg("json")
        .output()
        .await?;
    if !output.status.success() {
        bail!("bootc status failed with status: {}", output.status);
    }

    Ok(serde_json::from_slice(&output.stdout)?)
}

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

struct Bluerose {}

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

    async fn apply_usr_overlay(&mut self) -> zbus::fdo::Result<()> {
        bootc::apply_usr_overlay()
            .await
            .map_err(|e| zbus::fdo::Error::Failed(e.to_string()))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // upgrade().await?;
    let bluerose = Bluerose {};
    let _conn = connection::Builder::system()?
        .name("com.fyralabs.Bluerose")?
        .serve_at("/com/fyralabs/Bluerose", bluerose)?
        .build()
        .await?;

    pending::<()>().await;

    Ok(())
}
