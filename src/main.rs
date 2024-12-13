#![feature(never_type)]
#![feature(let_chains)]

use anyhow::Result;
use indicatif::{MultiProgress, ProgressBar};
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use std::ffi::OsStr;
use std::fs::File;
use std::io::Write;
use std::process::{Command, Stdio};
use std::str;
use std::sync::Arc;
use std::time::Instant;
use std::{fs, io, thread};
use tokio::sync::{mpsc, oneshot, watch};

pub fn command<S: AsRef<OsStr>>(program: S) -> Command {
  let mut cmd = Command::new(program);
  cmd.stderr(Stdio::null());
  cmd
}

fn video_frames(path: impl ToString) -> u64 {
  let output = command("ffprobe")
    .args([
      "-v",
      "error",
      "-select_streams",
      "v:0",
      "-count_frames",
      "-show_entries",
      "stream=nb_read_frames",
      "-of",
      "default=nokey=1:noprint_wrappers=1",
      path.to_string().as_str(),
    ])
    .output()
    .unwrap();

  str::from_utf8(&output.stdout).unwrap_or("0").trim().parse().unwrap_or(0)
}

fn split_video(input: &str, parts: usize) -> Vec<String> {
  let duration_output = command("ffprobe")
    .args([
      "-i",
      input,
      "-show_entries",
      "format=duration",
      "-v",
      "quiet",
      "-of",
      "csv=p=0",
    ])
    .output()
    .unwrap();
  let duration: f64 =
    String::from_utf8_lossy(&duration_output.stdout).trim().parse().unwrap();

  let segment = duration / parts as f64;

  let slice_part = |i| {
    let start = segment * i as f64;
    let path = format!("{i}.y4m");
    let _ = command("ffmpeg")
      .args([
        "-y",
        "-i",
        input,
        "-ss",
        &start.to_string(),
        "-t",
        &segment.to_string(),
        &path,
      ])
      .output()
      .unwrap();
    path
  };
  (0..parts).map(slice_part).collect::<Vec<_>>()
}

#[derive(Debug, Deserialize, Copy, Clone)]
pub enum Status {
  Frame(usize),
  Finish,
  None,
}

#[tokio::main]
async fn main() {
  let servers =
    (0..4).map(|i| format!("http://127.0.0.1:3{i:03}")).collect::<Vec<_>>();

  let path_parts = split_video("F:/Rust/av1r/zzz.mp4", servers.len());
  let parts =
    path_parts.iter().map(fs::read).collect::<io::Result<Vec<_>>>().unwrap();

  let instant = Instant::now();

  let (tx, mut rx) = mpsc::channel(32);
  let (stx, mut srx) = watch::channel(None::<(usize, Status)>);

  let reqwest = Arc::new(Client::new());
  for (i, (part, url)) in
    parts.into_iter().zip(servers.into_iter()).enumerate()
  {
    let server = url.clone();
    let (tx, client) = (tx.clone(), reqwest.clone());
    tokio::spawn(async move {
      let response =
        client.post(format!("{server}/encode")).body(part).send().await?;
      let bytes = response.bytes().await.unwrap();
      tx.send((i, bytes)).await.unwrap();
      Ok::<(), anyhow::Error>(())
    });

    let server = url.clone();
    let (stx, client) = (stx.clone(), reqwest.clone());
    tokio::spawn(async move {
      loop {
        let res =
          client.get(format!("{server}/status")).send().await?.json().await;
        let _ = stx.send(res.ok().map(|res| (i, res)));
      }

      Ok::<(), anyhow::Error>(())
    });
  }
  drop(tx);

  let (closer, close) = oneshot::channel();

  let mut multi = MultiProgress::new();
  let bars = path_parts
    .into_iter()
    .map(video_frames)
    .map(ProgressBar::new)
    .map(|bar| multi.add(bar))
    .collect::<Vec<_>>();
  thread::spawn(move || handle_bars(close, srx, bars));

  let mut parts = Vec::new();
  while let Some((part, bytes)) = rx.recv().await {
    fs::write(format!("{part}.ivf"), bytes).unwrap();
    parts.push(format!("{part}.ivf"));
  }
  let _ = closer.send(());

  let list = generate_file_list(&parts, "list.txt");
  let status = command("ffmpeg")
    .args([
      "-y", "-f", "concat", "-safe", "0", "-i", "list.txt", "-c", "copy",
      "out.mp4",
    ])
    .status()
    .expect("Failed to execute FFmpeg");

  for part in parts {
    let _ = fs::remove_file(part);
  }

  println!("ELAPSED: {:?}", instant.elapsed());
}

fn generate_file_list(files: &[String], list_file: &str) {
  let mut file = File::create(list_file).expect("Failed to create file list");
  for f in files {
    writeln!(file, "file '{}'", f).expect("Failed to write to file list");
  }
}

fn handle_bars(
  mut close: oneshot::Receiver<()>,
  srx: watch::Receiver<Option<(usize, Status)>>, mut bars: Vec<ProgressBar>,
) -> Result<()> {
  loop {
    if close.try_recv().is_ok() {
      for bar in &mut bars {
        bar.finish();
      }
    }

    if srx.has_changed()?
      && let Some((n, status)) = *srx.borrow()
    {
      match status {
        Status::Frame(frame) => bars[n].set_position(frame as u64),
        Status::Finish => {
          bars[n].finish();
        }
        Status::None => {}
      }
      for bar in &mut bars {
        bar.tick();
      }
    }

    if bars.iter().all(ProgressBar::is_finished) {
      break Ok(());
    }
  }
}
