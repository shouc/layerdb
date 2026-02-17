use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Context;
use futures_util::StreamExt;
use minio::s3::builders::ObjectToDelete;
use minio::s3::client::Client;
use minio::s3::creds::StaticProvider;
use minio::s3::error::{Error as MinioError, ErrorCode};
use minio::s3::http::BaseUrl;
use minio::s3::types::{S3Api, ToStream};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryOptions {
    pub max_attempts: u32,
    pub base_delay: Duration,
}

impl Default for RetryOptions {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            base_delay: Duration::from_millis(100),
        }
    }
}

impl RetryOptions {
    fn normalized(self) -> Self {
        Self {
            max_attempts: self.max_attempts.max(1),
            base_delay: if self.base_delay.is_zero() {
                Duration::from_millis(1)
            } else {
                self.base_delay
            },
        }
    }

    fn backoff_for_attempt(self, attempt: u32) -> Duration {
        let shift = attempt.saturating_sub(1).min(8);
        let multiplier = 1u32 << shift;
        self.base_delay.saturating_mul(multiplier)
    }
}

pub trait ObjectStore: Send + Sync {
    fn put(&self, key: &str, data: &[u8]) -> anyhow::Result<Option<String>>;
    fn get(&self, key: &str) -> anyhow::Result<Vec<u8>>;
    fn delete(&self, key: &str) -> anyhow::Result<()>;
    fn exists(&self, key: &str) -> anyhow::Result<bool>;
    fn list(&self, prefix: &str) -> anyhow::Result<Vec<String>>;
}

#[derive(Debug, Clone)]
pub enum S3ObjectStore {
    Local(LocalObjectStore),
    Minio(MinioObjectStore),
}

impl S3ObjectStore {
    pub fn for_options(
        options: Option<crate::db::S3Options>,
        local_root: impl Into<PathBuf>,
    ) -> anyhow::Result<Self> {
        if let Some(options) = options {
            Ok(Self::Minio(MinioObjectStore::new(options)?))
        } else {
            Ok(Self::Local(LocalObjectStore::new(local_root)?))
        }
    }

    pub fn put(&self, key: &str, data: &[u8]) -> anyhow::Result<Option<String>> {
        match self {
            Self::Local(store) => store.put(key, data),
            Self::Minio(store) => store.put(key, data),
        }
    }

    pub fn get(&self, key: &str) -> anyhow::Result<Vec<u8>> {
        match self {
            Self::Local(store) => store.get(key),
            Self::Minio(store) => store.get(key),
        }
    }

    pub fn delete(&self, key: &str) -> anyhow::Result<()> {
        match self {
            Self::Local(store) => store.delete(key),
            Self::Minio(store) => store.delete(key),
        }
    }

    pub fn exists(&self, key: &str) -> anyhow::Result<bool> {
        match self {
            Self::Local(store) => store.exists(key),
            Self::Minio(store) => store.exists(key),
        }
    }

    pub fn list(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
        match self {
            Self::Local(store) => store.list(prefix),
            Self::Minio(store) => store.list(prefix),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LocalObjectStore {
    root: PathBuf,
}

impl LocalObjectStore {
    pub fn new(root: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    fn key_path(&self, key: &str) -> PathBuf {
        let clean = key.trim_start_matches('/');
        self.root.join(clean)
    }
}

impl ObjectStore for LocalObjectStore {
    fn put(&self, key: &str, data: &[u8]) -> anyhow::Result<Option<String>> {
        let path = self.key_path(key);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = path.with_extension(format!("tmp.{}", std::process::id()));
        std::fs::write(&tmp, data)?;
        std::fs::rename(&tmp, &path)?;
        Ok(None)
    }

    fn get(&self, key: &str) -> anyhow::Result<Vec<u8>> {
        Ok(std::fs::read(self.key_path(key))?)
    }

    fn delete(&self, key: &str) -> anyhow::Result<()> {
        let path = self.key_path(key);
        if path.exists() {
            std::fs::remove_file(path)?;
        }
        Ok(())
    }

    fn exists(&self, key: &str) -> anyhow::Result<bool> {
        Ok(self.key_path(key).exists())
    }

    fn list(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
        let prefix = prefix.trim_start_matches('/');
        let start = self.root.join(prefix);
        if !start.exists() {
            return Ok(Vec::new());
        }

        let mut files = Vec::new();
        collect_files_recursively(&self.root, &start, &mut files)?;
        files.sort();
        Ok(files)
    }
}

fn collect_files_recursively(
    root: &Path,
    path: &Path,
    out: &mut Vec<String>,
) -> anyhow::Result<()> {
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_files_recursively(root, &path, out)?;
            continue;
        }

        let rel = path
            .strip_prefix(root)
            .with_context(|| format!("strip prefix for {}", path.display()))?;
        out.push(rel.to_string_lossy().replace('\\', "/"));
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct MinioObjectStore {
    client: Client,
    bucket: String,
    region: String,
    prefix: String,
    retry: RetryOptions,
}

impl MinioObjectStore {
    pub fn new(options: crate::db::S3Options) -> anyhow::Result<Self> {
        let endpoint = if options.endpoint.contains("://") {
            options.endpoint.clone()
        } else if options.secure {
            format!("https://{}", options.endpoint)
        } else {
            format!("http://{}", options.endpoint)
        };
        let base_url: BaseUrl = endpoint
            .parse()
            .with_context(|| format!("invalid s3 endpoint: {}", options.endpoint))?;
        let provider = StaticProvider::new(&options.access_key, &options.secret_key, None);
        let client = Client::new(base_url, Some(Box::new(provider)), None, None)
            .context("create minio client")?;

        let store = Self {
            client,
            bucket: options.bucket,
            region: options.region,
            prefix: options
                .prefix
                .trim_matches('/')
                .to_string(),
            retry: RetryOptions {
                max_attempts: options.retry_max_attempts,
                base_delay: Duration::from_millis(options.retry_base_delay_ms),
            }
            .normalized(),
        };

        store.ensure_bucket(options.auto_create_bucket)?;
        Ok(store)
    }

    fn ensure_bucket(&self, auto_create: bool) -> anyhow::Result<()> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let exists = self.with_retry("bucket_exists", move || {
            let client = client.clone();
            let bucket = bucket.clone();
            async move { Ok(client.bucket_exists(bucket).send().await?.exists) }
        })?;

        if exists {
            return Ok(());
        }
        if !auto_create {
            anyhow::bail!("s3 bucket does not exist and auto-create is disabled: {}", self.bucket);
        }

        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let region = self.region.clone();
        self.with_retry("create_bucket", move || {
            let client = client.clone();
            let bucket = bucket.clone();
            let region = region.clone();
            async move {
                client
                    .create_bucket(bucket)
                    .region(Some(region))
                    .send()
                    .await?;
                Ok(())
            }
        })?;
        Ok(())
    }

    fn full_key(&self, key: &str) -> String {
        let key = key.trim_start_matches('/');
        if self.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}/{}", self.prefix, key)
        }
    }

    fn with_retry<T, F, Fut>(&self, op_name: &str, mut op: F) -> anyhow::Result<T>
    where
        T: Send + 'static,
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, MinioError>> + Send + 'static,
    {
        let attempts = self.retry.max_attempts.max(1);
        let mut last_err: Option<anyhow::Error> = None;
        for attempt in 1..=attempts {
            match run_minio_future(op()) {
                Ok(value) => return Ok(value),
                Err(err) => {
                    last_err = Some(err.context(format!(
                        "{op_name} attempt {attempt}/{attempts} failed"
                    )));
                    if attempt < attempts {
                        std::thread::sleep(self.retry.backoff_for_attempt(attempt));
                    }
                }
            }
        }

        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("{op_name} failed without an error")))
    }
}

impl ObjectStore for MinioObjectStore {
    fn put(&self, key: &str, data: &[u8]) -> anyhow::Result<Option<String>> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let full_key = self.full_key(key);
        let data = data.to_vec();
        self.with_retry("put_object", move || {
            let client = client.clone();
            let bucket = bucket.clone();
            let full_key = full_key.clone();
            let data = data.clone();
            async move {
                let resp = client
                    .put_object_content(bucket, full_key, data)
                    .send()
                    .await?;
                Ok(resp.version_id)
            }
        })
    }

    fn get(&self, key: &str) -> anyhow::Result<Vec<u8>> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let full_key = self.full_key(key);
        self.with_retry("get_object", move || {
            let client = client.clone();
            let bucket = bucket.clone();
            let full_key = full_key.clone();
            async move {
                let resp = client.get_object(bucket, full_key).send().await?;
                let bytes = resp
                    .content
                    .to_segmented_bytes()
                    .await
                    .map_err(MinioError::from)?
                    .to_bytes()
                    .to_vec();
                Ok(bytes)
            }
        })
    }

    fn delete(&self, key: &str) -> anyhow::Result<()> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let full_key = self.full_key(key);
        self.with_retry("delete_object", move || {
            let client = client.clone();
            let bucket = bucket.clone();
            let full_key = full_key.clone();
            async move {
                client
                    .delete_object(bucket, ObjectToDelete::from(full_key))
                    .send()
                    .await?;
                Ok(())
            }
        })
    }

    fn exists(&self, key: &str) -> anyhow::Result<bool> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let full_key = self.full_key(key);
        self.with_retry("stat_object", move || {
            let client = client.clone();
            let bucket = bucket.clone();
            let full_key = full_key.clone();
            async move {
                match client.stat_object(bucket, full_key).send().await {
                    Ok(_) => Ok(true),
                    Err(MinioError::S3Error(err)) if err.code == ErrorCode::NoSuchKey => Ok(false),
                    Err(err) => Err(err),
                }
            }
        })
    }

    fn list(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let full_prefix = self.full_key(prefix);
        let configured_prefix = self.prefix.clone();
        self.with_retry("list_objects", move || {
            let client = client.clone();
            let bucket = bucket.clone();
            let full_prefix = full_prefix.clone();
            let configured_prefix = configured_prefix.clone();
            async move {
                let mut stream = client
                    .list_objects(bucket)
                    .prefix(Some(full_prefix))
                    .recursive(true)
                    .to_stream()
                    .await;

                let mut keys = Vec::new();
                while let Some(next) = stream.next().await {
                    let resp = next?;
                    for entry in resp.contents {
                        if entry.is_prefix || entry.is_delete_marker {
                            continue;
                        }
                        let key = if configured_prefix.is_empty() {
                            Some(entry.name)
                        } else {
                            entry
                                .name
                                .strip_prefix(&(configured_prefix.clone() + "/"))
                                .map(ToOwned::to_owned)
                        };
                        if let Some(key) = key {
                            keys.push(key);
                        }
                    }
                }

                keys.sort();
                Ok(keys)
            }
        })
    }
}

fn run_minio_future<T, Fut>(future: Fut) -> anyhow::Result<T>
where
    T: Send + 'static,
    Fut: Future<Output = Result<T, MinioError>> + Send + 'static,
{
    if tokio::runtime::Handle::try_current().is_ok() {
        let join = std::thread::spawn(move || -> anyhow::Result<T> {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .context("build tokio runtime for minio operation")?;
            runtime.block_on(future).map_err(anyhow::Error::from)
        });

        match join.join() {
            Ok(result) => result,
            Err(_) => Err(anyhow::anyhow!("minio runtime thread panicked")),
        }
    } else {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("build tokio runtime for minio operation")?;
        runtime.block_on(future).map_err(anyhow::Error::from)
    }
}

#[cfg(test)]
mod tests {
    use super::{LocalObjectStore, ObjectStore};

    #[test]
    fn local_store_put_get_list_delete() -> anyhow::Result<()> {
        let dir = tempfile::TempDir::new()?;
        let store = LocalObjectStore::new(dir.path())?;

        assert!(!store.exists("L1/a/meta.bin")?);
        store.put("L1/a/meta.bin", b"hello")?;
        store.put("L1/a/sb_00000000.bin", b"world")?;

        assert!(store.exists("L1/a/meta.bin")?);
        assert_eq!(store.get("L1/a/meta.bin")?, b"hello");

        let listed = store.list("L1/a")?;
        assert_eq!(listed.len(), 2);
        assert!(listed.contains(&"L1/a/meta.bin".to_string()));
        assert!(listed.contains(&"L1/a/sb_00000000.bin".to_string()));

        store.delete("L1/a/meta.bin")?;
        assert!(!store.exists("L1/a/meta.bin")?);
        assert!(store.exists("L1/a/sb_00000000.bin")?);
        Ok(())
    }
}
