//! Prompt dictation support.
//!
//! Non-Android platforms use a fully local pipeline built on sherpa-onnx:
//! cpal microphone capture, Silero VAD speech segmentation, and the
//! multilingual NeMo Parakeet TDT 0.6b v3 offline recognizer. Models are
//! downloaded once into the user cache and then reused without any API key or
//! network call during dictation.
//!
//! The pipeline runs in a separate worker process (the hidden `voice-worker`
//! subcommand of this same binary, see [`run_voice_worker`]). The native
//! speech stack can raise foreign C++ exceptions across the FFI boundary or
//! abort outright when system libraries are incompatible or model files are
//! corrupt; Rust cannot catch those, so in-process use would `SIGABRT` the
//! whole TUI. Isolating dictation in a child process turns any such crash
//! into a status-line warning while the chat session keeps running. The
//! worker streams progress to the parent as JSON lines on stdout, and stops
//! when its stdin reaches EOF (cancellation or parent exit).

use anyhow::Result;
#[cfg(target_os = "android")]
use anyhow::bail;

#[cfg(not(target_os = "android"))]
mod backend {
    use anyhow::{Context, Result, bail};
    use cpal::SampleFormat;
    use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
    use sherpa_onnx::{
        LinearResampler, OfflineRecognizer, OfflineRecognizerConfig, VadModelConfig,
        VoiceActivityDetector,
    };
    use std::{
        fs,
        io::{Read, Write},
        path::{Path, PathBuf},
        sync::mpsc,
        thread,
        time::{Duration, Instant},
    };

    const ASR_MODEL_BASE_URL: &str =
        "https://huggingface.co/csukuangfj/sherpa-onnx-nemo-parakeet-tdt-0.6b-v3-int8/resolve/main";
    const ASR_MODEL_DIR: &str = "sherpa-onnx-nemo-parakeet-tdt-0.6b-v3-int8";
    const ASR_ENCODER: &str = "encoder.int8.onnx";
    const ASR_DECODER: &str = "decoder.int8.onnx";
    const ASR_JOINER: &str = "joiner.int8.onnx";
    const ASR_TOKENS: &str = "tokens.txt";
    const VAD_MODEL_URL: &str =
        "https://github.com/k2-fsa/sherpa-onnx/releases/download/asr-models/silero_vad.onnx";
    const VAD_MODEL_FILE: &str = "silero_vad.onnx";

    pub(super) const DICTATION_TIMEOUT: Duration = Duration::from_secs(600);
    const DICTATION_SILENCE: Duration = Duration::from_secs(20);
    /// cpal delivers callbacks continuously (silence arrives as zeros), so a
    /// stream that produces no frames at all is broken, not quiet.
    const NO_AUDIO_TIMEOUT: Duration = Duration::from_secs(5);

    const SAMPLE_RATE: i32 = 16000;
    const VAD_WINDOW_SIZE: usize = 512;
    const INTERIM_DECODE_INTERVAL: Duration = Duration::from_millis(250);
    const LEVEL_EMIT_INTERVAL: Duration = Duration::from_millis(80);

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(super) struct ModelPaths {
        pub dir: PathBuf,
        pub encoder: PathBuf,
        pub decoder: PathBuf,
        pub joiner: PathBuf,
        pub tokens: PathBuf,
        pub vad: PathBuf,
    }

    impl ModelPaths {
        pub(super) fn in_cache(cache_root: PathBuf) -> Self {
            let voice = cache_root.join("voice");
            let dir = voice.join(ASR_MODEL_DIR);
            Self {
                encoder: dir.join(ASR_ENCODER),
                decoder: dir.join(ASR_DECODER),
                joiner: dir.join(ASR_JOINER),
                tokens: dir.join(ASR_TOKENS),
                vad: voice.join(VAD_MODEL_FILE),
                dir,
            }
        }

        pub(super) fn is_installed(&self) -> bool {
            has_model_data(&self.encoder)
                && has_model_data(&self.decoder)
                && has_model_data(&self.joiner)
                && has_model_data(&self.tokens)
                && has_model_data(&self.vad)
        }
    }

    /// A zero-byte model file is as unusable as a missing one; an interrupted
    /// download or extraction can leave either behind.
    pub(super) fn has_model_data(path: &Path) -> bool {
        fs::metadata(path)
            .map(|meta| meta.is_file() && meta.len() > 0)
            .unwrap_or(false)
    }

    fn mjolnir_cache_dir() -> Result<PathBuf> {
        dirs::cache_dir()
            .map(|dir| dir.join("mj"))
            .context("locate user cache directory")
    }

    pub(super) fn model_paths() -> Result<ModelPaths> {
        Ok(ModelPaths::in_cache(mjolnir_cache_dir()?))
    }

    /// Stream a URL to `dest`, reporting (downloaded, total) byte counts.
    ///
    /// reqwest::blocking creates its own Tokio runtime, which panics when
    /// called from within an existing Tokio context. Downloading on a plain OS
    /// thread guarantees there is no ambient runtime, regardless of call site;
    /// progress flows back over a channel so the callback need not be Send.
    fn download_to_file<F>(url: &str, dest: &Path, mut on_progress: F) -> Result<()>
    where
        F: FnMut(u64, Option<u64>),
    {
        let url = url.to_string();
        let dest = dest.to_path_buf();
        let (progress_tx, progress_rx) = mpsc::channel::<(u64, Option<u64>)>();
        let worker = thread::spawn(move || -> Result<()> {
            let mut response = reqwest::blocking::Client::builder()
                .user_agent("mjolnir-voice-setup")
                .build()
                .context("build download client")?
                .get(&url)
                .send()
                .with_context(|| format!("GET {url}"))?
                .error_for_status()
                .with_context(|| format!("download {url}"))?;
            let total = response.content_length();
            let mut file =
                fs::File::create(&dest).with_context(|| format!("create {}", dest.display()))?;
            let mut buffer = [0u8; 64 * 1024];
            let mut downloaded = 0u64;
            loop {
                let read = response.read(&mut buffer).context("read download body")?;
                if read == 0 {
                    break;
                }
                file.write_all(&buffer[..read])
                    .with_context(|| format!("write {}", dest.display()))?;
                downloaded += read as u64;
                let _ = progress_tx.send((downloaded, total));
            }
            Ok(())
        });
        for (downloaded, total) in progress_rx {
            on_progress(downloaded, total);
        }
        worker
            .join()
            .map_err(|_| anyhow::anyhow!("download thread panicked"))?
    }

    fn megabytes(bytes: u64) -> u64 {
        bytes / (1024 * 1024)
    }

    fn asr_model_url(file_name: &str) -> String {
        format!("{ASR_MODEL_BASE_URL}/{file_name}")
    }

    fn download_model_file<H>(file_name: &str, dest: &Path, on_status: &mut H) -> Result<()>
    where
        H: FnMut(String),
    {
        let tmp = dest.with_extension("part");
        let _ = fs::remove_file(&tmp);
        let mut last_percent = u64::MAX;
        if let Err(error) =
            download_to_file(&asr_model_url(file_name), &tmp, |downloaded, total| {
                let message = match total {
                    Some(total) if total > 0 => {
                        let percent = downloaded * 100 / total;
                        if percent == last_percent {
                            return;
                        }
                        last_percent = percent;
                        format!(
                            "downloading voice model: {file_name} {percent}% of {} MB",
                            megabytes(total)
                        )
                    }
                    _ => format!(
                        "downloading voice model: {file_name} {} MB",
                        megabytes(downloaded)
                    ),
                };
                on_status(message);
            })
        {
            let _ = fs::remove_file(&tmp);
            return Err(error).with_context(|| format!("download {file_name}"));
        }
        fs::rename(&tmp, dest).with_context(|| format!("install {}", dest.display()))
    }

    pub(super) fn ensure_models_installed<H>(paths: &ModelPaths, on_status: &mut H) -> Result<()>
    where
        H: FnMut(String),
    {
        if paths.is_installed() {
            return Ok(());
        }

        let voice_dir = paths
            .dir
            .parent()
            .context("resolve voice model cache parent")?;
        fs::create_dir_all(voice_dir).with_context(|| format!("create {}", voice_dir.display()))?;

        if !has_model_data(&paths.vad) {
            on_status("downloading voice activity model...".to_string());
            let tmp = paths.vad.with_extension("onnx.part");
            if let Err(error) = download_to_file(VAD_MODEL_URL, &tmp, |_, _| {}) {
                let _ = fs::remove_file(&tmp);
                return Err(error).context("download silero VAD model");
            }
            fs::rename(&tmp, &paths.vad)
                .with_context(|| format!("install {}", paths.vad.display()))?;
        }

        if !(has_model_data(&paths.encoder)
            && has_model_data(&paths.decoder)
            && has_model_data(&paths.joiner)
            && has_model_data(&paths.tokens))
        {
            let archive = voice_dir.join(format!("{ASR_MODEL_DIR}.tar.bz2.part"));
            let staging = voice_dir.join(format!("{ASR_MODEL_DIR}.extracting"));
            if archive.exists() || staging.exists() {
                on_status("clearing incomplete voice model archive setup...".to_string());
                let _ = fs::remove_file(&archive);
                let _ = fs::remove_dir_all(&staging);
            }
            fs::create_dir_all(&paths.dir)
                .with_context(|| format!("create {}", paths.dir.display()))?;
            if !has_model_data(&paths.encoder) {
                download_model_file(ASR_ENCODER, &paths.encoder, on_status)?;
            }
            if !has_model_data(&paths.decoder) {
                download_model_file(ASR_DECODER, &paths.decoder, on_status)?;
            }
            if !has_model_data(&paths.joiner) {
                download_model_file(ASR_JOINER, &paths.joiner, on_status)?;
            }
            if !has_model_data(&paths.tokens) {
                download_model_file(ASR_TOKENS, &paths.tokens, on_status)?;
            }
        }

        if !paths.is_installed() {
            bail!(
                "voice model installation under {} is incomplete; delete the directory and retry",
                paths.dir.display()
            );
        }
        Ok(())
    }

    pub(super) fn create_vad(paths: &ModelPaths) -> Result<VoiceActivityDetector> {
        let mut config = VadModelConfig::default();
        config.silero_vad.model = Some(paths.vad.display().to_string());
        config.silero_vad.threshold = 0.5;
        config.silero_vad.min_silence_duration = 0.25;
        config.silero_vad.min_speech_duration = 0.25;
        config.silero_vad.max_speech_duration = 5.0;
        config.silero_vad.window_size = VAD_WINDOW_SIZE as i32;
        config.sample_rate = SAMPLE_RATE;
        VoiceActivityDetector::create(&config, 60.0).context("create voice activity detector")
    }

    pub(super) fn create_recognizer(paths: &ModelPaths) -> Result<OfflineRecognizer> {
        let mut config = OfflineRecognizerConfig::default();
        config.model_config.transducer.encoder = Some(paths.encoder.display().to_string());
        config.model_config.transducer.decoder = Some(paths.decoder.display().to_string());
        config.model_config.transducer.joiner = Some(paths.joiner.display().to_string());
        config.model_config.tokens = Some(paths.tokens.display().to_string());
        config.model_config.model_type = Some("nemo_transducer".to_string());
        config.model_config.num_threads = decode_threads();
        OfflineRecognizer::create(&config).context("load voice recognition model")
    }

    fn decode_threads() -> i32 {
        thread::available_parallelism()
            .map(|n| n.get().min(4) as i32)
            .unwrap_or(2)
    }

    /// Samples from the capture callback, or the stream error that ended it.
    /// cpal reports stream failures through a separate callback; carrying them
    /// on the same channel lets the capture loop fail loudly instead of
    /// waiting out the silence timeout with a dead microphone.
    type AudioMessage = std::result::Result<Vec<f32>, String>;

    /// Build a cpal input stream that forwards mono f32 samples at the
    /// device's native rate.
    fn build_input_stream(
        device: &cpal::Device,
        tx: mpsc::Sender<AudioMessage>,
    ) -> Result<(cpal::Stream, i32)> {
        let supported = device
            .default_input_config()
            .context("query microphone input format")?;
        let config = supported.config();
        let sample_format = supported.sample_format();
        let channels = config.channels.max(1) as usize;
        let sample_rate = config.sample_rate.0 as i32;
        let make_err_fn = || {
            let tx = tx.clone();
            move |err: cpal::StreamError| {
                let _ = tx.send(Err(err.to_string()));
            }
        };

        let stream = match sample_format {
            SampleFormat::F32 => {
                let err_fn = make_err_fn();
                device.build_input_stream(
                    &config,
                    move |data: &[f32], _| {
                        let _ = tx.send(Ok(downmix(data.iter().copied(), channels)));
                    },
                    err_fn,
                    None,
                )
            }
            SampleFormat::I16 => {
                let err_fn = make_err_fn();
                device.build_input_stream(
                    &config,
                    move |data: &[i16], _| {
                        let samples = data.iter().map(|&s| s as f32 / i16::MAX as f32);
                        let _ = tx.send(Ok(downmix(samples, channels)));
                    },
                    err_fn,
                    None,
                )
            }
            SampleFormat::U16 => {
                let err_fn = make_err_fn();
                device.build_input_stream(
                    &config,
                    move |data: &[u16], _| {
                        let samples = data.iter().map(|&s| (s as f32 - 32768.0) / 32768.0);
                        let _ = tx.send(Ok(downmix(samples, channels)));
                    },
                    err_fn,
                    None,
                )
            }
            other => bail!("unsupported microphone sample format: {other:?}"),
        }
        .context("open microphone input stream")?;
        Ok((stream, sample_rate))
    }

    fn downmix<I>(samples: I, channels: usize) -> Vec<f32>
    where
        I: Iterator<Item = f32>,
    {
        let frames: Vec<f32> = samples.collect();
        frames
            .chunks(channels)
            .map(|frame| frame.iter().sum::<f32>() / channels as f32)
            .collect()
    }

    /// Normalize a raw RMS value into the 0.0..=1.0 meter range used by the UI.
    pub(super) fn normalized_level(rms: f32) -> f32 {
        (rms * 18.0).clamp(0.0, 1.0)
    }

    fn chunk_rms(samples: &[f32]) -> f32 {
        if samples.is_empty() {
            return 0.0;
        }
        let sum: f32 = samples.iter().map(|s| s * s).sum();
        (sum / samples.len() as f32).sqrt()
    }

    /// Join finalized utterances and the in-progress interim transcript.
    pub(super) fn compose_transcript(finalized: &[String], interim: &str) -> String {
        let mut parts: Vec<&str> = finalized
            .iter()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect();
        let interim = interim.trim();
        if !interim.is_empty() {
            parts.push(interim);
        }
        parts.join(" ")
    }

    pub(super) fn run<F, G, H>(
        mut on_partial: F,
        mut on_level: G,
        mut on_status: H,
        cancel_rx: mpsc::Receiver<()>,
    ) -> Result<String>
    where
        F: FnMut(String),
        G: FnMut(f32),
        H: FnMut(String),
    {
        let paths = model_paths()?;
        ensure_models_installed(&paths, &mut on_status)?;

        on_status("loading voice model...".to_string());
        let load_hint = || {
            format!(
                "load voice models (if this keeps failing, delete {} and retry)",
                paths.dir.display()
            )
        };
        let vad = create_vad(&paths).with_context(load_hint)?;
        let recognizer = create_recognizer(&paths).with_context(load_hint)?;

        // Model loading takes a moment; honor a cancellation that arrived in
        // the meantime without ever opening the microphone.
        if cancel_rx.try_recv().is_ok() {
            return Ok(String::new());
        }

        let host = cpal::default_host();
        let device = host
            .default_input_device()
            .context("no microphone input device was found")?;
        let (audio_tx, audio_rx) = mpsc::channel::<AudioMessage>();
        let (stream, mic_sample_rate) = build_input_stream(&device, audio_tx)?;
        let resampler = if mic_sample_rate != SAMPLE_RATE {
            Some(
                LinearResampler::create(mic_sample_rate, SAMPLE_RATE)
                    .context("create microphone resampler")?,
            )
        } else {
            None
        };
        stream.play().context("start microphone capture")?;
        on_status("listening...".to_string());

        let started_at = Instant::now();
        let mut received_audio = false;
        let mut last_activity_at = Instant::now();
        let mut last_level_at = Instant::now() - LEVEL_EMIT_INTERVAL;
        let mut last_interim_decode_at = Instant::now();

        let mut buffer = Vec::<f32>::new();
        let mut vad_offset = 0usize;
        let mut speech_started = false;

        let mut finalized = Vec::<String>::new();
        let mut interim = String::new();
        let mut last_emitted: Option<String> = None;
        let mut cancelled = false;

        loop {
            if cancel_rx.try_recv().is_ok() {
                cancelled = true;
                break;
            }
            if started_at.elapsed() >= DICTATION_TIMEOUT {
                break;
            }
            if last_activity_at.elapsed() >= DICTATION_SILENCE {
                break;
            }

            match audio_rx.recv_timeout(Duration::from_millis(30)) {
                Ok(Ok(samples)) => {
                    received_audio = true;
                    if last_level_at.elapsed() >= LEVEL_EMIT_INTERVAL {
                        on_level(normalized_level(chunk_rms(&samples)));
                        last_level_at = Instant::now();
                    }
                    match &resampler {
                        Some(resampler) => {
                            buffer.extend_from_slice(&resampler.resample(&samples, false))
                        }
                        None => buffer.extend_from_slice(&samples),
                    }
                }
                Ok(Err(message)) => bail!("microphone capture failed: {message}"),
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    bail!("microphone capture stopped unexpectedly")
                }
            }

            if !received_audio && started_at.elapsed() >= NO_AUDIO_TIMEOUT {
                bail!(
                    "microphone delivered no audio within {} seconds; it may be muted, in use \
                     by another application, or the audio backend may be incompatible",
                    NO_AUDIO_TIMEOUT.as_secs()
                );
            }

            while vad_offset + VAD_WINDOW_SIZE <= buffer.len() {
                vad.accept_waveform(&buffer[vad_offset..vad_offset + VAD_WINDOW_SIZE]);
                if vad.detected() {
                    last_activity_at = Instant::now();
                    if !speech_started {
                        speech_started = true;
                        last_interim_decode_at = Instant::now();
                    }
                }
                vad_offset += VAD_WINDOW_SIZE;
            }

            if !speech_started && buffer.len() > 10 * VAD_WINDOW_SIZE {
                let keep_from = buffer.len() - 10 * VAD_WINDOW_SIZE;
                buffer.drain(..keep_from);
                vad_offset = vad_offset.saturating_sub(keep_from);
            }

            if speech_started && last_interim_decode_at.elapsed() >= INTERIM_DECODE_INTERVAL {
                interim = decode_segment(&recognizer, SAMPLE_RATE, &buffer);
                last_interim_decode_at = Instant::now();
            }

            while !vad.is_empty() {
                if let Some(segment) = vad.front() {
                    let text = decode_segment(&recognizer, SAMPLE_RATE, segment.samples());
                    if !text.is_empty() {
                        finalized.push(text);
                        last_activity_at = Instant::now();
                    }
                }
                vad.pop();
                buffer.clear();
                vad_offset = 0;
                speech_started = false;
                interim.clear();
            }

            let transcript = compose_transcript(&finalized, &interim);
            if !transcript.is_empty() && last_emitted.as_deref() != Some(transcript.as_str()) {
                on_partial(transcript.clone());
                last_emitted = Some(transcript);
            }
        }

        drop(stream);

        if !cancelled {
            vad.flush();
            while !vad.is_empty() {
                if let Some(segment) = vad.front() {
                    let text = decode_segment(&recognizer, SAMPLE_RATE, segment.samples());
                    if !text.is_empty() {
                        finalized.push(text);
                        interim.clear();
                    }
                }
                vad.pop();
            }
        }

        let text = compose_transcript(&finalized, &interim);
        if !cancelled && text.is_empty() {
            bail!("no speech was recognized");
        }
        Ok(text)
    }

    pub(super) fn decode_segment(
        recognizer: &OfflineRecognizer,
        sample_rate: i32,
        samples: &[f32],
    ) -> String {
        if samples.is_empty() {
            return String::new();
        }
        let stream = recognizer.create_stream();
        stream.accept_waveform(sample_rate, samples);
        recognizer.decode(&stream);
        stream
            .get_result()
            .map(|result| result.text.trim().to_string())
            .unwrap_or_default()
    }
}

#[cfg(not(target_os = "android"))]
mod worker {
    use super::backend;
    use anyhow::{Context, Result, anyhow};
    use serde::{Deserialize, Serialize};
    use std::io::{BufRead, BufReader, Read, Write};
    use std::process::{Child, Command, ExitStatus, Stdio};
    use std::sync::mpsc;
    use std::thread;
    use std::time::{Duration, Instant};

    /// One JSON line on the worker's stdout.
    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    #[serde(tag = "event", rename_all = "snake_case")]
    pub(super) enum WorkerEvent {
        Status { message: String },
        Partial { text: String },
        Level { value: f32 },
        Result { text: String },
        Error { message: String },
    }

    pub(super) fn parse_event(line: &str) -> Option<WorkerEvent> {
        serde_json::from_str(line.trim()).ok()
    }

    fn emit(event: &WorkerEvent) {
        if let Ok(line) = serde_json::to_string(event) {
            let mut stdout = std::io::stdout().lock();
            let _ = writeln!(stdout, "{line}");
            let _ = stdout.flush();
        }
    }

    /// Child-process entry point for the hidden `voice-worker` subcommand.
    ///
    /// Runs the whole dictation pipeline and reports through [`WorkerEvent`]
    /// JSON lines on stdout. Stdin EOF (or any line on it) cancels dictation;
    /// the parent holds stdin open for the session's lifetime, so a dying
    /// parent also stops the worker instead of leaving the microphone open.
    pub(super) fn run_worker() -> i32 {
        let (cancel_tx, cancel_rx) = mpsc::channel();
        thread::spawn(move || {
            let mut line = String::new();
            let _ = std::io::stdin().lock().read_line(&mut line);
            let _ = cancel_tx.send(());
        });
        match backend::run(
            |text| emit(&WorkerEvent::Partial { text }),
            |value| emit(&WorkerEvent::Level { value }),
            |message| emit(&WorkerEvent::Status { message }),
            cancel_rx,
        ) {
            Ok(text) => {
                emit(&WorkerEvent::Result { text });
                0
            }
            Err(error) => {
                emit(&WorkerEvent::Error {
                    message: format!("{error:#}"),
                });
                1
            }
        }
    }

    /// How long after cancellation the worker gets to flush a final
    /// transcript before it is killed.
    const CANCEL_GRACE: Duration = Duration::from_secs(10);
    /// The worker enforces the dictation timeout itself; the parent allows
    /// some slack on top before declaring it hung.
    const WORKER_GRACE: Duration = Duration::from_secs(30);

    /// Parent-side dictation: spawn this binary's `voice-worker` subcommand
    /// and relay its events, so a native-library crash cannot take down the
    /// TUI process.
    pub(super) fn run<F, G, H>(
        on_partial: F,
        on_level: G,
        on_status: H,
        cancel_rx: mpsc::Receiver<()>,
    ) -> Result<String>
    where
        F: FnMut(String),
        G: FnMut(f32),
        H: FnMut(String),
    {
        let exe = std::env::current_exe().context("locate the mj executable")?;
        let child = Command::new(&exe)
            .arg("voice-worker")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("start voice worker {}", exe.display()))?;
        drive_worker(child, on_partial, on_level, on_status, cancel_rx)
    }

    /// Relay worker events to the UI callbacks and translate every way the
    /// worker can end — result, reported error, crash, or hang — into a
    /// `Result`. Non-protocol stdout lines (native-library noise) are ignored.
    pub(super) fn drive_worker<F, G, H>(
        mut child: Child,
        mut on_partial: F,
        mut on_level: G,
        mut on_status: H,
        cancel_rx: mpsc::Receiver<()>,
    ) -> Result<String>
    where
        F: FnMut(String),
        G: FnMut(f32),
        H: FnMut(String),
    {
        let mut stdin = child.stdin.take();
        let stdout = child
            .stdout
            .take()
            .context("voice worker stdout was not captured")?;
        let stderr = child.stderr.take();

        // None marks stdout EOF: the worker is gone without a verdict.
        let (event_tx, event_rx) = mpsc::channel::<Option<WorkerEvent>>();
        thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if let Some(event) = parse_event(&line)
                    && event_tx.send(Some(event)).is_err()
                {
                    return;
                }
            }
            let _ = event_tx.send(None);
        });
        let stderr_reader = stderr.map(|stderr| thread::spawn(move || read_tail(stderr)));

        let started_at = Instant::now();
        let mut cancelled_at: Option<Instant> = None;
        let mut last_partial = String::new();
        let outcome = loop {
            if cancelled_at.is_none() && cancel_rx.try_recv().is_ok() {
                cancelled_at = Some(Instant::now());
                // Closing stdin is the cancellation signal; the worker then
                // flushes and reports whatever it recognized so far.
                stdin = None;
            }
            if let Some(at) = cancelled_at
                && at.elapsed() >= CANCEL_GRACE
            {
                let _ = child.kill();
                break Some(Ok(last_partial.clone()));
            }
            if started_at.elapsed() >= backend::DICTATION_TIMEOUT + WORKER_GRACE {
                let _ = child.kill();
                break Some(Err(anyhow!("voice dictation timed out")));
            }
            match event_rx.recv_timeout(Duration::from_millis(50)) {
                Ok(Some(WorkerEvent::Partial { text })) => {
                    last_partial.clone_from(&text);
                    on_partial(text);
                }
                Ok(Some(WorkerEvent::Level { value })) => on_level(value),
                Ok(Some(WorkerEvent::Status { message })) => on_status(message),
                Ok(Some(WorkerEvent::Result { text })) => break Some(Ok(text)),
                Ok(Some(WorkerEvent::Error { message })) => break Some(Err(anyhow!(message))),
                Ok(None) | Err(mpsc::RecvTimeoutError::Disconnected) => break None,
                Err(mpsc::RecvTimeoutError::Timeout) => {}
            }
        };
        drop(stdin);

        let status = reap(child);
        let stderr_tail = stderr_reader
            .and_then(|reader| reader.join().ok())
            .unwrap_or_default();
        match outcome {
            Some(result) => result,
            None => {
                if !stderr_tail.trim().is_empty() {
                    tracing::warn!("voice worker stderr: {stderr_tail}");
                }
                Err(worker_crash_error(status, &stderr_tail))
            }
        }
    }

    /// Wait briefly for the worker to exit, killing it if it lingers.
    fn reap(mut child: Child) -> Option<ExitStatus> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match child.try_wait() {
                Ok(Some(status)) => return Some(status),
                Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(25)),
                _ => {
                    let _ = child.kill();
                    return child.wait().ok();
                }
            }
        }
    }

    /// Keep the last few KB of the worker's stderr for crash diagnostics.
    fn read_tail<R: Read>(mut reader: R) -> String {
        let mut buffer = Vec::new();
        let mut chunk = [0u8; 1024];
        while let Ok(read) = reader.read(&mut chunk) {
            if read == 0 {
                break;
            }
            buffer.extend_from_slice(&chunk[..read]);
            if buffer.len() > 8192 {
                let cut = buffer.len() - 4096;
                buffer.drain(..cut);
            }
        }
        String::from_utf8_lossy(&buffer).into_owned()
    }

    /// The worker vanished without reporting a result or an error: a native
    /// crash (foreign exception, abort) or an outright kill. Explain what
    /// happened without taking the session with it.
    pub(super) fn worker_crash_error(
        status: Option<ExitStatus>,
        stderr_tail: &str,
    ) -> anyhow::Error {
        let mut message = match status {
            Some(status) => format!("voice dictation {}", describe_exit(status)),
            None => "voice dictation stopped unexpectedly".to_string(),
        };
        if let Some(line) = last_meaningful_line(stderr_tail) {
            message.push_str(&format!(": {line}"));
        }
        let cache = backend::model_paths()
            .map(|paths| paths.dir.display().to_string())
            .unwrap_or_else(|_| "the voice model cache".to_string());
        message.push_str(&format!(
            " — the dictation engine runs in a separate process, so your session is unaffected; \
             this usually means an incompatible or outdated system library (try updating system \
             packages, or delete {cache} and retry)"
        ));
        anyhow!(message)
    }

    #[cfg(unix)]
    fn describe_exit(status: ExitStatus) -> String {
        use std::os::unix::process::ExitStatusExt;
        if let Some(signal) = status.signal() {
            let name = match signal {
                4 => " (SIGILL)",
                6 => " (SIGABRT)",
                8 => " (SIGFPE)",
                11 => " (SIGSEGV)",
                _ => "",
            };
            return format!("crashed with signal {signal}{name}");
        }
        describe_exit_code(status)
    }

    #[cfg(not(unix))]
    fn describe_exit(status: ExitStatus) -> String {
        describe_exit_code(status)
    }

    fn describe_exit_code(status: ExitStatus) -> String {
        match status.code() {
            Some(code) => format!("exited unexpectedly (code {code})"),
            None => "stopped unexpectedly".to_string(),
        }
    }

    fn last_meaningful_line(stderr_tail: &str) -> Option<String> {
        let line = stderr_tail
            .lines()
            .rev()
            .map(str::trim)
            .find(|line| !line.is_empty())?;
        let truncated: String = line.chars().take(200).collect();
        Some(truncated)
    }
}

/// Capture microphone audio and return the recognized transcript.
///
/// `on_partial` receives the cumulative transcript as it grows, `on_level`
/// receives normalized microphone levels for the input meter, and `on_status`
/// receives transient progress messages (model download, loading). Sending on
/// `cancel_rx` stops capture and returns whatever was recognized so far.
///
/// Dictation runs in a separate worker process (see the module docs); a crash
/// in the native speech stack surfaces here as an error instead of aborting
/// the TUI.
#[cfg(not(target_os = "android"))]
pub fn run_dictation<F, G, H>(
    on_partial: F,
    on_level: G,
    on_status: H,
    cancel_rx: std::sync::mpsc::Receiver<()>,
) -> Result<String>
where
    F: FnMut(String),
    G: FnMut(f32),
    H: FnMut(String),
{
    worker::run(on_partial, on_level, on_status, cancel_rx)
}

/// Entry point for the hidden `voice-worker` subcommand; returns the process
/// exit code.
#[cfg(not(target_os = "android"))]
pub fn run_voice_worker() -> i32 {
    worker::run_worker()
}

/// The worker subcommand exists on every platform so the CLI shape is
/// uniform, but Android has no dictation backend to run.
#[cfg(target_os = "android")]
pub fn run_voice_worker() -> i32 {
    eprintln!("voice dictation is not supported on Android");
    1
}

#[cfg(target_os = "android")]
pub fn run_dictation<F, G, H>(
    _on_partial: F,
    _on_level: G,
    _on_status: H,
    _cancel_rx: std::sync::mpsc::Receiver<()>,
) -> Result<String>
where
    F: FnMut(String),
    G: FnMut(f32),
    H: FnMut(String),
{
    bail!("voice dictation is not supported on Android")
}

pub fn dictation_error_message(error: &anyhow::Error) -> String {
    let message = error.to_string();
    if message.starts_with("voice") || message.starts_with("no speech") {
        return message;
    }
    if message.contains("microphone") {
        return format!("voice dictation could not use the microphone: {message}");
    }
    format!("voice dictation failed: {message}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(not(target_os = "android"))]
    use std::path::PathBuf;

    /// End-to-end check of the local model pipeline: downloads the real models
    /// into the user cache, loads the recognizer and VAD, and decodes a test
    /// wav shipped with the model archive.
    #[cfg(not(target_os = "android"))]
    #[test]
    #[ignore = "downloads ~0.7 GB of models; run with: cargo test -- --ignored"]
    fn dictation_models_install_and_decode_test_wav() {
        let paths = backend::model_paths().expect("resolve model paths");
        backend::ensure_models_installed(&paths, &mut |status| eprintln!("{status}"))
            .expect("install voice models");

        backend::create_vad(&paths).expect("load VAD model");
        let recognizer = backend::create_recognizer(&paths).expect("load recognizer");

        let wav_dir = paths.dir.join("test_wavs");
        let wav = std::fs::read_dir(&wav_dir)
            .expect("list test_wavs")
            .flatten()
            .map(|entry| entry.path())
            .find(|path| path.extension().is_some_and(|ext| ext == "wav"))
            .expect("find a test wav");
        let wave =
            sherpa_onnx::Wave::read(wav.to_str().expect("utf-8 wav path")).expect("read test wav");

        let text = backend::decode_segment(&recognizer, wave.sample_rate(), wave.samples());
        eprintln!("decoded {}: {text}", wav.display());
        assert!(!text.is_empty(), "expected a non-empty transcript");
    }

    #[cfg(not(target_os = "android"))]
    #[test]
    fn model_paths_are_under_voice_cache() {
        let paths = backend::ModelPaths::in_cache(PathBuf::from("/cache/mj"));
        let voice = PathBuf::from("/cache/mj").join("voice");
        assert_eq!(
            paths.dir,
            voice.join("sherpa-onnx-nemo-parakeet-tdt-0.6b-v3-int8")
        );
        assert_eq!(paths.encoder, paths.dir.join("encoder.int8.onnx"));
        assert_eq!(paths.decoder, paths.dir.join("decoder.int8.onnx"));
        assert_eq!(paths.joiner, paths.dir.join("joiner.int8.onnx"));
        assert_eq!(paths.tokens, paths.dir.join("tokens.txt"));
        assert_eq!(paths.vad, voice.join("silero_vad.onnx"));
    }

    #[cfg(not(target_os = "android"))]
    #[test]
    fn compose_transcript_joins_finalized_and_interim() {
        let finalized = vec!["Hello there.".to_string(), "How are you?".to_string()];
        assert_eq!(
            backend::compose_transcript(&finalized, "I am"),
            "Hello there. How are you? I am"
        );
        assert_eq!(
            backend::compose_transcript(&finalized, "  "),
            "Hello there. How are you?"
        );
        assert_eq!(backend::compose_transcript(&[], ""), "");
    }

    #[cfg(not(target_os = "android"))]
    #[test]
    fn normalized_level_clamps_to_meter_range() {
        assert_eq!(backend::normalized_level(0.0), 0.0);
        assert_eq!(backend::normalized_level(1.0), 1.0);
        assert!(backend::normalized_level(0.01) > 0.0);
        assert!(backend::normalized_level(0.01) < 1.0);
    }

    #[test]
    fn error_messages_are_prefixed_for_context() {
        let err = anyhow::anyhow!("some backend exploded");
        assert_eq!(
            dictation_error_message(&err),
            "voice dictation failed: some backend exploded"
        );
        let err = anyhow::anyhow!("no speech was recognized");
        assert_eq!(dictation_error_message(&err), "no speech was recognized");
        let err = anyhow::anyhow!("voice dictation is not supported on Android");
        assert_eq!(
            dictation_error_message(&err),
            "voice dictation is not supported on Android"
        );
    }

    #[cfg(not(target_os = "android"))]
    #[test]
    fn worker_events_round_trip_as_json_lines() {
        use super::worker::{WorkerEvent, parse_event};
        let events = [
            WorkerEvent::Status {
                message: "loading voice model...".to_string(),
            },
            WorkerEvent::Partial {
                text: "hello".to_string(),
            },
            WorkerEvent::Level { value: 0.25 },
            WorkerEvent::Result {
                text: "hello world".to_string(),
            },
            WorkerEvent::Error {
                message: "microphone capture failed".to_string(),
            },
        ];
        for event in events {
            let line = serde_json::to_string(&event).expect("serialize event");
            assert!(!line.contains('\n'), "protocol lines must be single-line");
            assert_eq!(parse_event(&line), Some(event));
        }
    }

    #[cfg(not(target_os = "android"))]
    #[test]
    fn parse_event_ignores_non_protocol_output() {
        use super::worker::parse_event;
        assert_eq!(parse_event(""), None);
        assert_eq!(parse_event("onnxruntime init log line"), None);
        assert_eq!(parse_event("{\"event\":\"unknown\"}"), None);
    }

    #[cfg(not(target_os = "android"))]
    #[test]
    fn zero_byte_model_files_are_not_installed() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let paths = backend::ModelPaths::in_cache(tmp.path().to_path_buf());
        std::fs::create_dir_all(&paths.dir).expect("create model dir");
        let files = [
            &paths.encoder,
            &paths.decoder,
            &paths.joiner,
            &paths.tokens,
            &paths.vad,
        ];
        for path in files {
            std::fs::write(path, b"").expect("write empty file");
        }
        assert!(
            !paths.is_installed(),
            "zero-byte model files must not count as installed"
        );
        for path in files {
            std::fs::write(path, b"model data").expect("write file");
        }
        assert!(paths.is_installed());
    }

    #[cfg(not(target_os = "android"))]
    #[test]
    fn crash_error_includes_stderr_line_and_recovery_hint() {
        let err = super::worker::worker_crash_error(
            None,
            "onnx init\nfatal runtime error: Rust cannot catch foreign exceptions, aborting\n",
        );
        let message = err.to_string();
        assert!(message.contains("voice dictation stopped unexpectedly"));
        assert!(message.contains("Rust cannot catch foreign exceptions"));
        assert!(message.contains("session is unaffected"));
    }

    /// Fake-worker tests: drive_worker against short shell scripts standing
    /// in for the real worker, covering each way the child can end.
    #[cfg(all(unix, not(target_os = "android")))]
    mod fake_worker {
        use super::super::worker::drive_worker;
        use std::process::{Command, Stdio};
        use std::sync::mpsc;

        fn spawn_fake(script: &str) -> std::process::Child {
            Command::new("/bin/sh")
                .arg("-c")
                .arg(script)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("spawn fake worker")
        }

        fn drive(
            script: &str,
            cancel_rx: mpsc::Receiver<()>,
        ) -> (anyhow::Result<String>, Vec<String>, Vec<String>) {
            let mut partials = Vec::new();
            let mut statuses = Vec::new();
            let result = drive_worker(
                spawn_fake(script),
                |text| partials.push(text),
                |_level| {},
                |message| statuses.push(message),
                cancel_rx,
            );
            (result, partials, statuses)
        }

        fn never_cancelled() -> mpsc::Receiver<()> {
            let (tx, rx) = mpsc::channel();
            std::mem::forget(tx);
            rx
        }

        #[test]
        fn forwards_events_and_returns_result() {
            let script = r#"
                printf '%s\n' '{"event":"status","message":"listening..."}'
                printf '%s\n' '{"event":"level","value":0.5}'
                printf '%s\n' '{"event":"partial","text":"hello"}'
                printf 'native library noise\n'
                printf '%s\n' '{"event":"result","text":"hello world"}'
            "#;
            let (result, partials, statuses) = drive(script, never_cancelled());
            assert_eq!(result.expect("transcript"), "hello world");
            assert_eq!(partials, vec!["hello".to_string()]);
            assert_eq!(statuses, vec!["listening...".to_string()]);
        }

        #[test]
        fn error_event_surfaces_as_error() {
            let script = r#"
                printf '%s\n' '{"event":"error","message":"microphone capture failed: boom"}'
                exit 1
            "#;
            let (result, _, _) = drive(script, never_cancelled());
            let message = result.expect_err("error").to_string();
            assert_eq!(message, "microphone capture failed: boom");
        }

        #[test]
        fn abort_is_contained_and_described() {
            let script = r#"
                echo 'fatal runtime error: Rust cannot catch foreign exceptions, aborting' >&2
                kill -ABRT $$
            "#;
            let (result, _, _) = drive(script, never_cancelled());
            let message = result.expect_err("crash error").to_string();
            assert!(message.contains("signal 6"), "got: {message}");
            assert!(message.contains("SIGABRT"), "got: {message}");
            assert!(
                message.contains("Rust cannot catch foreign exceptions"),
                "got: {message}"
            );
            assert!(message.contains("session is unaffected"), "got: {message}");
        }

        #[test]
        fn silent_exit_is_reported_with_code() {
            let (result, _, _) = drive("exit 3", never_cancelled());
            let message = result.expect_err("exit error").to_string();
            assert!(
                message.contains("exited unexpectedly (code 3)"),
                "got: {message}"
            );
        }

        #[test]
        fn cancel_closes_stdin_and_returns_flushed_result() {
            // The fake worker mirrors the real cancellation handshake: wait
            // for stdin EOF, then flush a final transcript.
            let script = r#"
                while read -r _; do :; done
                printf '%s\n' '{"event":"result","text":"flushed"}'
            "#;
            let (cancel_tx, cancel_rx) = mpsc::channel();
            cancel_tx.send(()).expect("queue cancel");
            let (result, _, _) = drive(script, cancel_rx);
            assert_eq!(result.expect("flushed transcript"), "flushed");
        }
    }
}
