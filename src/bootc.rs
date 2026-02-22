use color_eyre::{Result, eyre::bail};
use tokio::process::Command;

use crate::types::host::Host;

pub async fn get_status() -> Result<Host> {
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

pub async fn rollback(apply: bool) -> Result<()> {
    let mut cmd = Command::new("bootc");
    let cmd = if apply {
        cmd.arg("rollback").arg("--apply")
    } else {
        cmd.arg("rollback")
    };
    let status = cmd
        .status()
        .await
        .map_err(|e| zbus::fdo::Error::SpawnFailed(e.to_string()))?;
    if !status.success() {
        bail!("bootc rollback failed with status: {status}");
    }

    Ok(())
}

pub async fn apply_usr_overlay() -> Result<()> {
    let status = Command::new("bootc")
        .arg("usr-overlay")
        .status()
        .await
        .map_err(|e| zbus::fdo::Error::SpawnFailed(e.to_string()))?;

    if !status.success() {
        bail!("bootc usr-overlay failed with status (likely already applied): {status}");
    }

    Ok(())
}
