use color_eyre::{Result, eyre::bail};
use command_fds::{CommandFdExt, FdMapping};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::unix::pipe::pipe,
    process::Command,
    sync::mpsc,
    task::JoinHandle,
};
use tokio_stream::wrappers::ReceiverStream;

use crate::types::host::Host;
use crate::types::progress::Event;

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

/// Starts `bootc upgrade` with `--progress-fd 3` and returns:
/// - A stream of parsed JSONL progress `Event`s.
/// - A join handle that resolves to the command `ExitStatus`.
///
/// The returned stream yields `Ok(Event)` for each progress message. If the
/// progress reader hits an error (I/O or JSON parse), it yields `Err(...)`
/// and then ends. If the consumer drops the stream, the reader task will stop
/// once the channel closes.
/// 
/// Example:
/// ```
/// let (mut events, cmd_handle) = bootc::upgrade().await?;
/// while let Some(event) = events.next().await {
///     match event? {
///         Event::Start { version } => println!("starting {version}"),
///         Event::ProgressSteps { description, steps, steps_total, .. } => {
///             println!("{description}: {steps}/{steps_total}");
///         }
///         Event::ProgressBytes { description, bytes, bytes_total, .. } => {
///             println!("{description}: {bytes}/{bytes_total}");
///         }
///     }
/// }
/// let status = cmd_handle.await??;
/// if !status.success() {
///     bail!("bootc upgrade failed with status: {status}");
/// }
/// # Ok::<_, color_eyre::Report>(())
/// ```
pub async fn upgrade() -> Result<(
    ReceiverStream<Result<Event>>,
    JoinHandle<Result<std::process::ExitStatus>>,
)> {
    let (tx, rx) = pipe()?;
    let (event_tx, event_rx) = mpsc::channel(64);

    let cmd_handle = tokio::spawn(async move {
        let status = Command::new("bootc")
            .arg("upgrade")
            .arg("--progress-fd")
            .arg("3")
            .fd_mappings(vec![FdMapping {
                parent_fd: tx.into_blocking_fd()?,
                child_fd: 3,
            }])?
            .status()
            .await?;
        Ok::<_, color_eyre::Report>(status)
    });

    let _reader_handle = tokio::spawn(async move {
        let reader = BufReader::new(rx);
        let mut lines = reader.lines();
        // let event_tx = event_tx;

        let result: Result<()> = async {
            while let Some(line) = lines.next_line().await? {
                let event: Event = serde_json::from_str(&line)?;
                if event_tx.send(Ok(event)).await.is_err() {
                    break;
                }
            }
            Ok(())
        }
        .await;

        if let Err(err) = result {
            let _ = event_tx.send(Err(err)).await;
        }
    });

    Ok((ReceiverStream::new(event_rx), cmd_handle))
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
