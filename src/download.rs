use std::fmt::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use futures::StreamExt;
use futures::TryStreamExt;
use indicatif::MultiProgress;
use indicatif::ProgressBar;
use indicatif::ProgressState;
use indicatif::ProgressStyle;
use librespot::core::session::Session;

use crate::encoder;
use crate::encoder::Format;
use crate::encoder::Samples;
use crate::stream::Stream;
use crate::stream::StreamEvent;
use crate::stream::StreamEventChannel;
use crate::track::Track;
use crate::track::TrackMetadata;

use tokio::sync::Mutex;
use tokio::time::sleep;

pub struct Downloader {
    session: Session,
    progress_bar: MultiProgress,
}

#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    pub base_delay: Duration,
    pub multiplier: f64,
    pub max_delay: Duration,
    pub reset_after_success: bool,
}

impl RateLimitConfig {
    pub fn new(base_delay: Duration, multiplier: f64, max_delay: Duration) -> Self {
        let multiplier = if multiplier.is_finite() {
            multiplier
        } else {
            1.0
        };
        let multiplier = multiplier.max(1.0);
        let max_delay = if max_delay < base_delay {
            base_delay
        } else {
            max_delay
        };
        RateLimitConfig {
            base_delay,
            multiplier,
            max_delay,
            reset_after_success: true,
        }
    }

    pub fn disabled() -> Self {
        RateLimitConfig {
            base_delay: Duration::ZERO,
            multiplier: 1.0,
            max_delay: Duration::ZERO,
            reset_after_success: true,
        }
    }

    pub fn is_enabled(&self) -> bool {
        !self.base_delay.is_zero()
    }
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        RateLimitConfig::disabled()
    }
}

#[derive(Debug, Clone)]
pub struct DownloadOptions {
    pub destination: PathBuf,
    pub parallel: usize,
    pub format: Format,
    pub force: bool,
    pub rate_limit: RateLimitConfig,
}

impl DownloadOptions {
    pub fn new(destination: Option<String>, parallel: usize, format: Format, force: bool) -> Self {
        let destination =
            destination.map_or_else(|| std::env::current_dir().unwrap(), PathBuf::from);
        DownloadOptions {
            destination,
            parallel,
            format,
            force,
            rate_limit: RateLimitConfig::default(),
        }
    }

    pub fn set_rate_limit(&mut self, rate_limit: RateLimitConfig) {
        self.rate_limit = rate_limit;
    }
}

#[derive(Clone)]
struct RateLimiter {
    config: RateLimitConfig,
    state: Arc<Mutex<RateLimiterState>>,
}

impl RateLimiter {
    fn new(config: RateLimitConfig) -> Self {
        RateLimiter {
            config,
            state: Arc::new(Mutex::new(RateLimiterState::default())),
        }
    }

    async fn wait_ready(&self) {
        if !self.config.is_enabled() {
            return;
        }

        loop {
            let sleep_duration = {
                let mut state = self.state.lock().await;
                if let Some(next_ready) = state.next_ready {
                    let now = Instant::now();
                    if next_ready > now {
                        Some(next_ready - now)
                    } else {
                        state.next_ready = None;
                        None
                    }
                } else {
                    None
                }
            };

            match sleep_duration {
                Some(duration) if !duration.is_zero() => sleep(duration).await,
                Some(_) => continue,
                None => break,
            }
        }
    }

    async fn on_failure(&self) -> Duration {
        if !self.config.is_enabled() {
            return Duration::ZERO;
        }

        let mut state = self.state.lock().await;
        let now = Instant::now();

        let next_delay = if state.current_delay.is_zero() {
            self.config.base_delay
        } else {
            let scaled = state.current_delay.as_secs_f64() * self.config.multiplier.max(1.0);
            let clamped = scaled.max(self.config.base_delay.as_secs_f64()).min(
                self.config
                    .max_delay
                    .max(self.config.base_delay)
                    .as_secs_f64(),
            );
            Duration::from_secs_f64(clamped)
        };

        if next_delay.is_zero() {
            state.current_delay = Duration::ZERO;
            state.next_ready = None;
            return Duration::ZERO;
        }

        state.current_delay = next_delay;
        let proposed_ready = now + next_delay;
        state.next_ready = match state.next_ready {
            Some(existing) if existing > proposed_ready => Some(existing),
            _ => Some(proposed_ready),
        };

        next_delay
    }

    async fn on_success(&self) {
        if !self.config.is_enabled() {
            return;
        }

        if !self.config.reset_after_success {
            return;
        }

        let mut state = self.state.lock().await;
        let now = Instant::now();
        if matches!(state.next_ready, Some(next) if next > now) {
            // Keep the pending sleep to let other tasks honor it.
            return;
        }

        state.current_delay = Duration::ZERO;
        state.next_ready = None;
    }
}

#[derive(Default)]
struct RateLimiterState {
    current_delay: Duration,
    next_ready: Option<Instant>,
}

impl Downloader {
    pub fn new(session: Session) -> Self {
        Downloader {
            session,
            progress_bar: MultiProgress::new(),
        }
    }

    pub async fn download_tracks(
        &self,
        tracks: Vec<Track>,
        options: &DownloadOptions,
    ) -> Result<()> {
        let rate_limiter = Arc::new(RateLimiter::new(options.rate_limit.clone()));

        futures::stream::iter(tracks)
            .map(|track| {
                let rate_limiter = Arc::clone(&rate_limiter);
                async move { self.download_track(track, options, rate_limiter).await }
            })
            .buffer_unordered(options.parallel)
            .try_collect::<Vec<_>>()
            .await?;

        Ok(())
    }

    #[tracing::instrument(name = "download_track", skip(self, options, rate_limiter))]
    async fn download_track(
        &self,
        track: Track,
        options: &DownloadOptions,
        rate_limiter: Arc<RateLimiter>,
    ) -> Result<()> {
        rate_limiter.wait_ready().await;

        let metadata = track.metadata(&self.session).await?;
        let track_label = metadata.to_string();
        tracing::info!("Downloading track: {:?}", metadata.track_name);

        let filename = format!("{}.{}", track_label, options.format.extension());
        let path = options
            .destination
            .join(&filename)
            .to_str()
            .ok_or(anyhow::anyhow!("Could not set the output path"))?
            .to_string();

        if !options.force && PathBuf::from(&path).exists() {
            tracing::info!(
                "Skipping {}, file already exists. Use --force to force re-downloading the track",
                &metadata.track_name
            );
            rate_limiter.on_success().await;
            return Ok(());
        }

        let pb = self.add_progress_bar(&metadata);

        let stream = Stream::new(self.session.clone());
        let channel = match stream.stream(track).await {
            Ok(channel) => channel,
            Err(e) => {
                self.fail_with_error(&pb, &track_label, e.to_string());
                self.backoff_after_failure(&rate_limiter, &track_label)
                    .await;
                return Ok(());
            }
        };

        let samples = match self.buffer_track(channel, &pb, &metadata).await {
            Ok(samples) => samples,
            Err(e) => {
                self.fail_with_error(&pb, &track_label, e.to_string());
                self.backoff_after_failure(&rate_limiter, &track_label)
                    .await;
                return Ok(());
            }
        };

        tracing::info!("Encoding track: {}", track_label);
        pb.set_message(format!("Encoding {}", track_label));

        let encoder = crate::encoder::get_encoder(options.format);
        let stream = encoder.encode(samples).await?;

        pb.set_message(format!("Writing {}", track_label));
        tracing::info!("Writing track: {:?} to file: {}", track_label, &path);
        stream.write_to_file(&path).await?;

        let tags = metadata.tags().await?;
        encoder::tags::store_tags(path, &tags, options.format).await?;

        pb.finish_with_message(format!("Downloaded {}", track_label));
        rate_limiter.on_success().await;
        Ok(())
    }

    fn add_progress_bar(&self, track: &TrackMetadata) -> ProgressBar {
        let pb = self
            .progress_bar
            .add(ProgressBar::new(track.approx_size() as u64));
        pb.enable_steady_tick(Duration::from_millis(100));
        pb.set_style(ProgressStyle::with_template("{spinner:.green} {msg} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({eta})")
            // Infallible
            .unwrap()
            .with_key("eta", |state: &ProgressState, w: &mut dyn Write| write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap())
            .progress_chars("#>-"));
        pb.set_message(track.to_string());
        pb
    }

    async fn buffer_track(
        &self,
        mut rx: StreamEventChannel,
        pb: &ProgressBar,
        metadata: &TrackMetadata,
    ) -> Result<Samples> {
        let mut samples = Vec::<i32>::new();
        while let Some(event) = rx.recv().await {
            match event {
                StreamEvent::Write {
                    bytes,
                    total,
                    mut content,
                } => {
                    tracing::trace!("Written {} bytes out of {}", bytes, total);
                    pb.set_position(bytes as u64);
                    samples.append(&mut content);
                }
                StreamEvent::Finished => {
                    tracing::info!("Finished downloading track");
                    break;
                }
                StreamEvent::Error(stream_error) => {
                    tracing::error!("Error while streaming track: {:?}", stream_error);
                    return Err(anyhow::anyhow!("Streaming error: {:?}", stream_error));
                }
                StreamEvent::Retry {
                    attempt,
                    max_attempts,
                } => {
                    tracing::warn!(
                        "Retrying download, attempt {} of {}: {}",
                        attempt,
                        max_attempts,
                        metadata.to_string()
                    );
                    pb.set_message(format!(
                        "Retrying ({}/{}) {}",
                        attempt,
                        max_attempts,
                        metadata.to_string()
                    ));
                }
            }
        }
        Ok(Samples {
            samples,
            ..Default::default()
        })
    }

    fn fail_with_error<S>(&self, pb: &ProgressBar, name: &str, e: S)
    where
        S: Into<String>,
    {
        tracing::error!("Failed to download {}: {}", name, e.into());
        pb.finish_with_message(
            console::style(format!("Failed! {}", name))
                .red()
                .to_string(),
        );
    }

    async fn backoff_after_failure(&self, rate_limiter: &RateLimiter, track_label: &str) {
        let delay = rate_limiter.on_failure().await;
        if delay.is_zero() {
            return;
        }

        let delay_ms = delay.as_millis() as u64;
        tracing::warn!(
            delay_ms,
            track = track_label,
            "Rate limit backoff triggered after download failure"
        );

        let message = format!(
            "[rate-limit] Backing off for {:.1}s after failure: {}",
            delay.as_secs_f32(),
            track_label
        );
        let _ = self.progress_bar.println(message);

        sleep(delay).await;
    }
}
