use color_eyre::{
    Result,
    eyre::{WrapErr, bail},
};
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

/// Spawns `bootc ... --progress-fd 3` and returns:
/// - a stream of parsed JSONL `Event`s read from fd 3
/// - a JoinHandle for the bootc exit status
pub fn spawn_bootc_with_progress(
    args: &[&str],
) -> Result<(
    ReceiverStream<Result<Event>>,
    JoinHandle<Result<std::process::ExitStatus>>,
)> {
    let (tx, rx) = pipe().wrap_err("creating unix pipe for progress-fd")?;

    let (event_tx, event_rx) = mpsc::channel::<Result<Event>>(64);
    let args: Vec<String> = args.iter().map(|s| (*s).to_owned()).collect();

    let cmd_handle: JoinHandle<Result<std::process::ExitStatus>> = tokio::spawn(async move {
        let mut cmd = Command::new("bootc");
        cmd.args(&args).arg("--progress-fd").arg("3");

        let owned_fd = tx
            .into_blocking_fd()
            .wrap_err("converting progress pipe sender into OwnedFd")?;

        cmd.fd_mappings(vec![FdMapping {
            parent_fd: owned_fd,
            child_fd: 3,
        }])
        .wrap_err("setting fd mapping for --progress-fd")?;

        let status = cmd.status().await.wrap_err("running bootc")?;
        Ok(status)
    });

    // Parse JSONL events
    tokio::spawn(async move {
        let mut lines = BufReader::new(rx).lines();

        loop {
            let line = match lines.next_line().await {
                Ok(Some(l)) => l,
                // dropping should just close the stream
                Ok(None) => break, // EOF: child closed fd 3
                Err(e) => {
                    let _ = event_tx.send(Err(e.into())).await;
                    break;
                }
            };

            if line.is_empty() {
                continue;
            }

            match serde_json::from_str::<Event>(&line) {
                Ok(ev) => {
                    if event_tx.send(Ok(ev)).await.is_err() {
                        break;
                    }
                }
                Err(e) => {
                    // Optional: include the raw line to make debugging schema mismatches easier.
                    let err = color_eyre::eyre::eyre!(
                        "failed to parse bootc progress JSONL: {e}; line={line}"
                    );
                    let _ = event_tx.send(Err(err)).await;
                    break;
                }
            }
        }
    });

    Ok((ReceiverStream::new(event_rx), cmd_handle))
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
    spawn_bootc_with_progress(&["upgrade"])
}

/// Similar to `upgrade()`, but for `bootc switch <img_ref>`.
/// The returned stream will yield progress events from the `switch` command.
pub async fn switch(
    img_ref: &str,
) -> Result<(
    ReceiverStream<Result<Event>>,
    JoinHandle<Result<std::process::ExitStatus>>,
)> {
    spawn_bootc_with_progress(&["switch", img_ref])
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
