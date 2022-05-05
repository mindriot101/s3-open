use eyre::{Report, WrapErr};
use std::io::{Seek, Write};
use std::str::FromStr;
use tokio_stream::StreamExt;

use clap::Parser;

type Result<T> = std::result::Result<T, Report>;

#[derive(Parser)]
struct Args {
    object: String,
}

#[derive(Debug)]
struct S3Info {
    bucket: String,
    key: String,
    extension: Option<String>,
}

impl FromStr for S3Info {
    type Err = Report;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        if !s.starts_with("s3://") {
            eyre::bail!("missing s3:// prefix");
        }

        let mut parts = s.strip_prefix("s3://").unwrap().split("/");
        let bucket = parts.next().ok_or_else(|| eyre::eyre!("missing bucket"))?;
        let key_parts: Vec<_> = parts.collect();
        let key = key_parts.join("/");
        let extension = key.rsplit_once('.').map(|(_, ext)| ext.to_string());

        Ok(Self {
            bucket: bucket.to_string(),
            key,
            extension,
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let s3_info: S3Info = args.object.parse().wrap_err("invalid S3 url")?;
    tracing::debug!(?s3_info, "extracted s3 information");

    let config = aws_config::load_from_env().await;
    let client = aws_sdk_s3::Client::new(&config);

    let mut res = client
        .get_object()
        .bucket(&s3_info.bucket)
        .key(&s3_info.key)
        .send()
        .await
        .wrap_err("fetching file from S3")?;

    let mut tf = {
        if let Some(ref ext) = s3_info.extension {
            tempfile::Builder::new()
                .suffix(&format!(".{ext}"))
                .tempfile()?
        } else {
            tempfile::NamedTempFile::new().wrap_err("creating temporary file")?
        }
    };
    tracing::debug!(path = ?tf.path(), "created temporary file");

    let mut bytes_written = 0;
    while let Some(bytes) = res.body.try_next().await? {
        bytes_written += tf.write(&bytes)?;
    }

    tf.seek(std::io::SeekFrom::Start(0))?;
    tracing::debug!(%bytes_written, "file contents written");

    // open editor
    let tfile_path = tf.path().as_os_str();
    let mut child = std::process::Command::new("nvim")
        .args(&[tfile_path])
        .spawn()
        .wrap_err("spawning editor")?;
    let status = child.wait().wrap_err("waiting for editor")?;
    if !status.success() {
        eyre::bail!("editor exited unsuccessfully");
    }

    Ok(())
}
