use chrono::{DateTime, Utc};
use color_eyre::{Result, eyre::bail};
use command_fds::{CommandFdExt, FdMapping};
use serde::{Deserialize, Serialize};
use std::future::pending;
use std::{
    error::Error,
    os::fd::{AsFd, AsRawFd},
};
use tokio::{
    io::{AsyncBufReadExt, BufReader, Lines},
    net::unix::pipe::pipe,
    process::Command,
};
use zbus::zvariant::Type;
use zbus::{connection, interface};

use crate::types::host::{BootEntry, Host, ImageStatus};
use crate::types::progress::Event;

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

#[derive(Deserialize, Serialize, Type, PartialEq, Debug)]
struct Image {
    pub image: String,
    pub digest: String,
    pub timestamp: Option<DateTime<Utc>>,
}

impl From<ImageStatus> for Image {
    fn from(status: ImageStatus) -> Self {
        Self {
            image: status.image.image,
            digest: status.image_digest,
            timestamp: status.timestamp,
        }
    }
}

struct Bluerose {}

#[interface(name = "com.fyralabs.Bluerose1")]
impl Bluerose {
    #[zbus(property)]
    async fn staged_image(&self) -> zbus::fdo::Result<(Image,)> {
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
    async fn booted_image(&mut self) -> zbus::fdo::Result<(Image,)> {
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
    async fn rollback_image(&mut self) -> zbus::fdo::Result<(Image,)> {
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

// async fn upgrade() -> Result<(), Box<dyn Error>> {
//     let (tx, rx) = pipe()?;
//     let cmd = Command::new("bootc")
//         .arg("upgrade")
//         .arg("--progress-fd")
//         .arg("3")
//         .fd_mappings(vec![FdMapping {
//             parent_fd: tx.into_blocking_fd()?,
//             child_fd: 3,
//         }])?
//         .status();

//     let reader = BufReader::new(rx);
//     let mut lines = reader.lines();

//     let reader = async {
//         while let Some(line) = lines.next_line().await? {
//             println!("Progress: {line}");
//             let event: types::progress::Event = serde_json::from_str(&line)?;
//             match event {
//                 Event::Start { version } => {
//                     dbg!(version);
//                 }
//                 Event::ProgressSteps {
//                     description,
//                     id,
//                     steps,
//                     steps_cached,
//                     steps_total,
//                     subtasks,
//                     task,
//                 } => {
//                     dbg!(
//                         description,
//                         id,
//                         steps,
//                         steps_cached,
//                         steps_total,
//                         subtasks,
//                         task
//                     );
//                 }
//                 Event::ProgressBytes {
//                     bytes,
//                     bytes_cached,
//                     bytes_total,
//                     description,
//                     id,
//                     steps,
//                     steps_cached,
//                     steps_total,
//                     subtasks,
//                     task,
//                 } => {
//                     dbg!(
//                         bytes,
//                         bytes_cached,
//                         bytes_total,
//                         description,
//                         id,
//                         steps,
//                         steps_cached,
//                         steps_total,
//                         subtasks,
//                         task
//                     );
//                 }
//             }
//         }

//         Ok(())
//     };

//     tokio::try_join!(cmd, reader)?;

//     Ok(())
// }

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
