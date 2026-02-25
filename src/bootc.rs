use color_eyre::{
    Result,
    eyre::{self, WrapErr, bail},
};
use command_fds::{CommandFdExt, FdMapping};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::unix::pipe::pipe,
    process::Command,
    sync::mpsc,
};
use tokio_stream::wrappers::ReceiverStream;
use zbus::zvariant::Type;

use crate::types::host::Host;
use crate::types::progress::Event;

// parsing of bootc upgrade --check
//
// This one is some raw human-readable stuff, so we can parse it with some trimming
// and pattern matching or something, and then convert it into a structured UpdateInfo that we can return to the caller.
/// Update information returned by `bootc upgrade --check`, which includes the image reference, sizes and digests of the available update.
///
/// This is parsed from the human-readable output of `bootc upgrade --check`, which looks something like this:
/// ```text
/// Update available for: docker://foo/bar:latest
///   Digest: sha256:abc123...
/// Total new layers: 3    Size: 1 MB
/// Removed layers: 1    Size: 500 KB
/// Added layers: 4    Size: 2 MB
/// ```
#[derive(Debug, Type, Serialize, Deserialize)]
pub struct UpdateInfo {
    pub image: String,
    pub digest: String,
    pub new_layers: usize,
    pub new_layers_size: String,
    pub removed_layers: usize,
    pub removed_layers_size: String,
    pub added_layers: usize,
    pub added_layers_size: String,
}

impl UpdateInfo {
    // wow this is kinda messy
    //
    // todo: Clean this up properly,
    // this one is quick and dirty but very hard to read.
    //
    // Hopefully by then bootc upgrade --check will have a machine-readable output format

    pub fn from_bootc_output(output: &str) -> Result<Option<Self>> {
        let mut lines = output.lines();

        let first_line = lines
            .next()
            .ok_or_else(|| eyre::eyre!("unexpected empty output from `bootc upgrade --check`"))?;

        if first_line.trim().starts_with("No changes in:") {
            return Ok(None);
        }

        let (image, digest) = if let Some((img_part, digest_part)) =
            first_line.split_once("Digest:")
        {
            let image = img_part
                .strip_prefix("Update available for:")
                .ok_or_else(|| {
                    eyre::eyre!("unexpected format of first line in `bootc upgrade --check` output")
                })?
                .trim()
                .to_string();
            let digest = digest_part.trim().to_string();
            (image, digest)
        } else if first_line.starts_with("Update available for:") {
            let image = first_line
                .strip_prefix("Update available for:")
                .ok_or_else(|| {
                    eyre::eyre!("unexpected format of first line in `bootc upgrade --check` output")
                })?
                .trim()
                .to_string();

            let second_line = lines.next().ok_or_else(|| {
                eyre::eyre!(
                    "unexpected format of `bootc upgrade --check` output: missing 'Digest:' line"
                )
            })?;

            let digest = second_line
                .trim_start()
                .strip_prefix("Digest:")
                .ok_or_else(|| {
                    eyre::eyre!(
                        "unexpected format of `bootc upgrade --check` output: missing 'Digest:'"
                    )
                })?
                .trim()
                .to_string();

            (image, digest)
        } else {
            return Err(eyre::eyre!(
                "unexpected format of first line in `bootc upgrade --check` output: missing 'Digest:'"
            ));
        };

        let mut new_layers = 0;
        let mut new_layers_size = String::new();
        let mut removed_layers = 0;
        let mut removed_layers_size = String::new();
        let mut added_layers = 0;
        let mut added_layers_size = String::new();

        for line in lines {
            if line.starts_with("Total new layers:") {
                if let Some((count_part, size_part)) =
                    line["Total new layers:".len()..].split_once("Size:")
                {
                    new_layers = count_part.trim().parse()?;
                    new_layers_size = size_part.trim().replace("\u{a0}", " ");
                }
            } else if line.starts_with("Removed layers:") {
                if let Some((count_part, size_part)) =
                    line["Removed layers:".len()..].split_once("Size:")
                {
                    removed_layers = count_part.trim().parse()?;
                    removed_layers_size = size_part.trim().replace("\u{a0}", " ");
                }
            } else if line.starts_with("Added layers:") {
                if let Some((count_part, size_part)) =
                    line["Added layers:".len()..].split_once("Size:")
                {
                    added_layers = count_part.trim().parse()?;
                    added_layers_size = size_part.trim().replace("\u{a0}", " ");
                }
            }
        }

        Ok(Some(Self {
            image,
            digest,
            new_layers,
            new_layers_size,
            removed_layers,
            removed_layers_size,
            added_layers,
            added_layers_size,
        }))
    }
}

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
/// - the spawned `bootc` child process handle
pub fn spawn_bootc_with_progress(
    args: &[&str],
) -> Result<(ReceiverStream<Result<Event>>, tokio::process::Child)> {
    let (tx, rx) = pipe().wrap_err("creating unix pipe for progress-fd")?;

    let (event_tx, event_rx) = mpsc::channel::<Result<Event>>(64);
    let args: Vec<String> = args.iter().map(|s| (*s).to_owned()).collect();

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

    let child = cmd.spawn().wrap_err("spawning bootc")?;

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

    Ok((ReceiverStream::new(event_rx), child))
}

pub async fn check_update() -> Result<Option<UpdateInfo>> {
    let output = Command::new("bootc")
        .arg("upgrade")
        .arg("--check")
        .output()
        .await
        .wrap_err("running `bootc upgrade --check`")?;

    if !output.status.success() {
        bail!(
            "`bootc upgrade --check` failed with status: {}; stderr: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let stdout = String::from_utf8(output.stdout)
        .wrap_err("parsing stdout from `bootc upgrade --check` as UTF-8")?;

    UpdateInfo::from_bootc_output(&stdout)
}

/// Starts `bootc upgrade` with `--progress-fd 3` and returns:
/// - A stream of parsed JSONL progress `Event`s.
/// - the spawned `bootc` child process handle.
///
/// The returned stream yields `Ok(Event)` for each progress message. If the
/// progress reader hits an error (I/O or JSON parse), it yields `Err(...)`
/// and then ends. If the consumer drops the stream, the reader task will stop
/// once the channel closes.
///
/// Example:
/// ```
/// let (mut events, mut child) = bootc::upgrade().await?;
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
/// let status = child.wait().await?;
/// if !status.success() {
///     bail!("bootc upgrade failed with status: {status}");
/// }
/// # Ok::<_, color_eyre::Report>(())
/// ```
pub async fn upgrade() -> Result<(ReceiverStream<Result<Event>>, tokio::process::Child)> {
    spawn_bootc_with_progress(&["upgrade"])
}

/// Similar to `upgrade()`, but for `bootc switch <img_ref>`.
/// The returned stream will yield progress events from the `switch` command.
pub async fn switch(
    img_ref: &str,
) -> Result<(ReceiverStream<Result<Event>>, tokio::process::Child)> {
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

#[test]
fn test_parse_update_info() {
    let output = include_str!("testdata/checkupdate.out");

    let info = UpdateInfo::from_bootc_output(output)
        .expect("failed to parse update info")
        .expect("expected update info");

    println!("{:#?}", info);
    assert_eq!(info.image, "docker://ghcr.io/ultramarine-linux/plasma-bootc:main");
    assert!(info.digest.starts_with("sha256:"));
    assert_eq!(info.new_layers, 100);
    assert_eq!(info.removed_layers, 100);
    assert_eq!(info.added_layers, 100);
}

#[test]
fn test_parse_update_info_no_update() {
    let output = "No changes in: docker://foo/bar:latest\n";

    let info = UpdateInfo::from_bootc_output(output).expect("failed to parse update info");

    println!("{:#?}", info);

    assert!(info.is_none(), "expected no update info, got: {:#?}", info);
}
