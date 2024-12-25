#![feature(never_type)]
#![feature(let_chains)]
#![feature(try_blocks)]

use {
  anyhow::Result,
  indicatif::{MultiProgress, ProgressBar},
  reqwest::Client,
  serde::Deserialize,
  std::{
    ffi::OsStr,
    fs::{self, File},
    future::Future,
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    str,
    sync::Arc,
    thread,
    time::{Duration, Instant},
  },
  tokio::{
    sync::{mpsc, oneshot, watch},
    time,
  },
};

pub fn command<S: AsRef<OsStr>>(program: S) -> Command {
  let mut cmd = Command::new(program);
  cmd.stderr(Stdio::null());
  cmd
}

fn video_frames(path: &Path) -> u64 {
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
    ])
    .arg(path)
    .output()
    .unwrap();

  str::from_utf8(&output.stdout).unwrap_or("0").trim().parse().unwrap_or(0)
}

fn split_video(
  input: &Path, parts: usize,
) -> impl Iterator<Item = (u64, Vec<u8>)> + '_ {
  let duration_output = command("ffprobe")
    .arg("-i")
    .arg(input)
    .args([
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

  let frames = video_frames(input);

  let segment = duration / parts as f64;

  let slice_part = move |i| {
    let start = segment * i as f64;

    let output = command("ffmpeg")
      .args(["-y", "-i"])
      .arg(input)
      .args([
        "-ss",
        &start.to_string(),
        "-t",
        &segment.to_string(),
        "-f",
        "ismv",
        "pipe:1",
      ])
      .output()
      .unwrap();
    (frames / parts as u64, output.stdout)
  };
  (0..parts).map(slice_part)
}

#[derive(Debug, Deserialize, Copy, Clone)]
pub enum Status {
  Frame(usize),
  Finish,
  None,
}

use clap::Parser;

#[derive(Parser)]
struct Args {
  input: PathBuf,

  #[arg(short)]
  output: PathBuf,

  #[arg(short, value_delimiter = ' ', num_args = 1..)]
  encoder: Vec<String>,
}

#[tokio::main]
async fn main() {
  match entry(Args::parse()).await {
    Ok(_) => {}
    Err(err) => eprintln!("{err:?}"),
  }
}

async fn entry(Args { input, output, encoder }: Args) -> Result<()> {
  let servers = encoder.into_iter().map(|s| (1, s)).collect::<Vec<_>>();
  let _ = File::open(&input)?;

  let (frames, parts): (Vec<_>, Vec<_>) =
    split_video(&input, servers.len()).unzip();

  let instant = Instant::now();

  let (tx, mut rx) = mpsc::channel(32);
  let (stx, mut srx) = watch::channel(None::<(u8, Status)>);

  let reqwest = Arc::new(Client::new());
  for (i, (part, (threads, server))) in
    parts.into_iter().zip(servers.into_iter()).enumerate()
  {
    let (tx, client) = (tx.clone(), reqwest.clone());
    let url = server.clone();
    tokio::spawn(async move {
      let result: Result<()> = try {
        let response = client
          .post(format!("{url}/encode?idx={i}&threads={threads}"))
          .body(part)
          .send()
          .await?;
        let bytes = response.bytes().await?;
        tx.send((i, bytes)).await.unwrap();
      };
      match result {
        Ok(_) => {}
        Err(err) => {
          println!("{err:?}");
        }
      }
    });

    let (stx, client) = (stx.clone(), reqwest.clone());
    let url = server.clone();
    tokio::spawn(async move {
      loop {
        let mut wait = time::interval(Duration::from_millis(50));

        let result: Result<_> = try {
          wait.tick().await;

          let status: (Option<u8>, Status) =
            client.get(format!("{url}/status")).send().await?.json().await?;

          status
        };
        match result {
          Ok((idx, status)) => {
            let _ = stx.send(idx.map(|idx| (idx, status)));
            if let Status::Finish = status {
              break;
            }
          }
          Err(err) => {
            println!("{err}");
          }
        }
      }
    });
  }
  drop(tx);

  let (closer, close) = oneshot::channel();

  let mut multi = MultiProgress::new();
  let bars = frames
    .into_iter()
    .map(ProgressBar::new)
    .map(|bar| multi.add(bar))
    .collect::<Vec<_>>();
  thread::spawn(move || handle_bars(close, srx, bars));
  multi.println("Encoding...").unwrap();

  let mut parts = Vec::new();
  while let Some((part, bytes)) = rx.recv().await {
    fs::write(format!("{part}.ivf"), bytes).unwrap();
    parts.push(format!("{part}.ivf"));
  }
  let _ = closer.send(());
  let _ = multi.clear();

  let list = generate_file_list(&parts, "list.txt");
  let status = command("ffmpeg")
    .args(["-y", "-f", "concat", "-safe", "0", "-i", "list.txt", "-c", "copy"])
    .arg(output)
    .status()
    .expect("Failed to execute FFmpeg");

  for part in parts {
    let _ = fs::remove_file(part);
  }

  println!("ELAPSED: {:?}", instant.elapsed());

  Ok(())
}

fn generate_file_list(files: &[String], list_file: &str) {
  let mut file = File::create(list_file).expect("Failed to create file list");
  for f in files {
    writeln!(file, "file '{}'", f).expect("Failed to write to file list");
  }
}

fn handle_bars(
  mut close: oneshot::Receiver<()>,
  srx: watch::Receiver<Option<(u8, Status)>>, mut bars: Vec<ProgressBar>,
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
        Status::Frame(frame) => bars[n as usize].set_position(frame as u64),
        Status::Finish => {
          bars[n as usize].finish();
        }
        Status::None => {}
      }
    }

    if bars.iter().all(ProgressBar::is_finished) {
      break Ok(());
    }
  }
}
