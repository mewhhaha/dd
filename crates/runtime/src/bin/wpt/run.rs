//! Runs one test file in a fresh worker isolate and collects its results.

use crate::manifest::{Job, Kind};
use base64::Engine;
use runtime::conformance::{Error, WorkerIsolate};
use serde::Deserialize;
use std::fs;
use std::future::poll_fn;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::task::Poll;
use std::time::{Duration, Instant};

const HARNESS_JS: &str = include_str!("harness.js");
const TESTHARNESS: &str = "resources/testharness.js";

/// How long the harness gets to report after it is timed out.
const GRACE: Duration = Duration::from_secs(2);
/// How long past that a script may run before its isolate is terminated.
const RUNAWAY: Duration = Duration::from_secs(5);

/// The WPT checkout, the only place test input comes from.
pub struct Checkout {
    root: PathBuf,
    testharness: String,
}

impl Checkout {
    pub fn open(root: &Path) -> Result<Self, String> {
        let root = root
            .canonicalize()
            .map_err(|error| format!("no WPT checkout at {}: {error}", root.display()))?;
        let checkout = Self {
            root,
            testharness: String::new(),
        };
        let testharness = checkout.read_text(TESTHARNESS)?;
        Ok(Self {
            testharness,
            ..checkout
        })
    }

    /// Resolves `reference`, a URL path (`/common/gc.js`) or a path relative
    /// to the directory `base`, to a path relative to the checkout.
    fn resolve(&self, base: &str, reference: &str) -> Result<String, String> {
        let reference = reference.split(['?', '#']).next().unwrap_or_default();
        let joined = match reference.strip_prefix('/') {
            Some(absolute) => absolute.to_string(),
            None => format!("{base}/{reference}"),
        };
        let mut parts: Vec<&str> = Vec::new();
        for part in joined.split('/') {
            match part {
                "" | "." => {}
                ".." => {
                    parts
                        .pop()
                        .ok_or_else(|| format!("{reference} points outside the checkout"))?;
                }
                part => parts.push(part),
            }
        }
        Ok(parts.join("/"))
    }

    /// Reads the file wptserve serves for `url_path` (relative to the root),
    /// refusing anything (a symlink, say) that leads outside the checkout.
    fn read(&self, url_path: &str) -> Result<Vec<u8>, String> {
        let relative = match url_path {
            // The one rewrite in wptserve's routes.
            "resources/WebIDLParser.js" => "resources/webidl2/lib/webidl2.js",
            path => path,
        };
        let path = self
            .root
            .join(relative)
            .canonicalize()
            .map_err(|error| format!("{relative}: {error}"))?;
        if !path.starts_with(&self.root) {
            return Err(format!("{relative} leads outside the checkout"));
        }
        fs::read(&path).map_err(|error| format!("{relative}: {error}"))
    }

    fn read_text(&self, relative: &str) -> Result<String, String> {
        self.read(relative)
            .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HarnessStatus {
    Ok,
    Error,
    Timeout,
    PreconditionFailed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubtestStatus {
    Pass,
    Fail,
    Timeout,
    NotRun,
    PreconditionFailed,
}

impl SubtestStatus {
    fn from_code(code: u8) -> Self {
        match code {
            0 => Self::Pass,
            2 => Self::Timeout,
            3 => Self::NotRun,
            4 => Self::PreconditionFailed,
            _ => Self::Fail,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Pass => "PASS",
            Self::Fail => "FAIL",
            Self::Timeout => "TIMEOUT",
            Self::NotRun => "NOTRUN",
            Self::PreconditionFailed => "PRECONDITION_FAILED",
        }
    }
}

impl HarnessStatus {
    fn from_code(code: u8) -> Self {
        match code {
            0 => Self::Ok,
            2 => Self::Timeout,
            3 => Self::PreconditionFailed,
            _ => Self::Error,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Ok => "OK",
            Self::Error => "ERROR",
            Self::Timeout => "TIMEOUT",
            Self::PreconditionFailed => "PRECONDITION_FAILED",
        }
    }
}

pub struct Subtest {
    pub name: String,
    pub status: SubtestStatus,
    pub message: Option<String>,
}

pub struct Outcome {
    pub harness: HarnessStatus,
    pub harness_message: Option<String>,
    pub subtests: Vec<Subtest>,
    pub duration: Duration,
}

#[derive(Deserialize)]
struct RawResults {
    harness: Option<RawHarness>,
    tests: Vec<RawSubtest>,
}

#[derive(Deserialize)]
struct RawHarness {
    status: Option<u8>,
    message: Option<String>,
}

#[derive(Deserialize)]
struct RawSubtest {
    name: String,
    status: u8,
    message: Option<String>,
}

#[derive(Deserialize)]
struct DataRequest {
    id: u64,
    path: String,
}

/// Why the harness did not finish on its own.
enum Cut {
    /// The per-file timeout passed.
    Timeout,
    /// Nothing was left to run while tests were still waiting.
    Stalled,
    /// A script would not yield and its isolate was terminated.
    Runaway,
}

pub async fn run_job(job: &Job, checkout: &Checkout, timeout: Duration) -> Outcome {
    let start = Instant::now();
    let mut outcome = match run_in_isolate(job, checkout, timeout).await {
        Ok(outcome) => outcome,
        Err(message) => Outcome {
            harness: HarnessStatus::Error,
            harness_message: Some(message),
            subtests: Vec::new(),
            duration: Duration::ZERO,
        },
    };
    outcome.duration = start.elapsed();
    outcome
}

async fn run_in_isolate(
    job: &Job,
    checkout: &Checkout,
    timeout: Duration,
) -> Result<Outcome, String> {
    let mut isolate = WorkerIsolate::new()
        .await
        .map_err(|error| format!("could not create the worker isolate: {error}"))?;
    // A script stuck in a loop never yields to the timeout below; this
    // thread terminates it instead.
    let terminate = isolate.terminate_handle();
    let (stop, stopped) = mpsc::channel::<()>();
    let limit = timeout + GRACE + RUNAWAY;
    let watchdog = std::thread::spawn(move || {
        if stopped.recv_timeout(limit) == Err(mpsc::RecvTimeoutError::Timeout) {
            terminate.terminate();
        }
    });
    let result = drive(&mut isolate, job, checkout, timeout).await;
    drop(stop);
    let _ = watchdog.join();
    let cut = result?;
    // The watchdog may have fired, at a runaway script or just as the run
    // ended; either way the results are still there to read.
    isolate.cancel_termination();
    let raw = isolate
        .execute_script("<wpt:results>", "__wpt_runner.results()")
        .map_err(|error| format!("could not read the results: {error}"))?;
    let raw: RawResults =
        serde_json::from_str(&raw).map_err(|error| format!("unreadable results: {error}"))?;
    let (harness, harness_message) = match raw.harness {
        Some(harness) => (
            harness
                .status
                .map_or(HarnessStatus::Error, HarnessStatus::from_code),
            harness.message,
        ),
        None => (
            HarnessStatus::Timeout,
            Some(
                match cut {
                    Some(Cut::Runaway) => "a script did not yield and was terminated",
                    _ => "the harness did not complete",
                }
                .to_string(),
            ),
        ),
    };
    let subtests = raw
        .tests
        .into_iter()
        .map(|test| Subtest {
            name: test.name,
            status: SubtestStatus::from_code(test.status),
            message: test.message,
        })
        .collect();
    Ok(Outcome {
        harness,
        harness_message,
        subtests,
        duration: Duration::ZERO,
    })
}

/// The URL wptserve would serve the test's worker script from.
fn test_url(job: &Job) -> String {
    let path = &job.file.path;
    let origin = if path.contains(".https.") {
        "https://web-platform.test:8443"
    } else {
        "http://web-platform.test:8000"
    };
    let script = match job.file.kind {
        Kind::Any => format!("{}.any.worker.js", path.trim_end_matches(".any.js")),
        Kind::Worker => path.clone(),
    };
    format!("{origin}/{script}{}", job.variant)
}

fn origin_of(url: &str) -> &str {
    let after_scheme = url.find("://").map_or(0, |index| index + 3);
    url[after_scheme..]
        .find('/')
        .map_or(url, |index| &url[..after_scheme + index])
}

/// Loads the test into the isolate and runs its event loop until the
/// harness completes. Returns why it was cut short, if it was.
async fn drive(
    isolate: &mut WorkerIsolate,
    job: &Job,
    checkout: &Checkout,
    timeout: Duration,
) -> Result<Option<Cut>, String> {
    let deadline = Instant::now() + timeout;
    let file = &job.file;
    let url = test_url(job);
    let origin = origin_of(&url).to_string();
    let base = file.path.rsplit_once('/').map_or("", |(dir, _)| dir);

    // The scripts the test runs, after testharness.js, in order.
    let mut scripts = Vec::new();
    let references = match file.kind {
        Kind::Any => &file.meta.scripts,
        Kind::Worker => &file.imports,
    };
    for reference in references {
        let path = checkout.resolve(base, reference)?;
        if path != TESTHARNESS {
            scripts.push(path);
        }
    }
    let imported = match file.kind {
        Kind::Any => serde_json::Value::Null,
        Kind::Worker => std::iter::once(TESTHARNESS)
            .chain(scripts.iter().map(String::as_str))
            .map(|path| serde_json::Value::String(format!("/{path}")))
            .collect(),
    };
    scripts.push(file.path.clone());

    let config = serde_json::json!({
        "url": url,
        "title": file.meta.title,
        "importedScripts": imported,
        // fetch's own tests see only dd's fetch.
        "serveDataFiles": !file.path.starts_with("fetch/"),
    });
    let setup = |isolate: &mut WorkerIsolate, name: &str, source: &str| {
        isolate
            .execute_script(name, source)
            .map(drop)
            .map_err(|error| format!("runner setup failed in {name}: {error}"))
    };
    setup(
        isolate,
        "<wpt:harness>",
        &format!("{HARNESS_JS}({config});"),
    )?;
    setup(
        isolate,
        &format!("{origin}/{TESTHARNESS}"),
        &checkout.testharness,
    )?;
    setup(isolate, "<wpt:attach>", "__wpt_runner.attach();")?;

    // Like importScripts in a worker: an error stops the scripts after it,
    // and reaches the harness as an `error` event.
    let mut loaded = true;
    for path in &scripts {
        let name = format!("{origin}/{path}");
        let result = checkout
            .read_text(path)
            .map_err(|error| Error::new(format!("NetworkError: could not load {name}: {error}")))
            .and_then(|source| isolate.execute_script(&name, &source).map(drop));
        if let Err(error) = result {
            if error.is_terminated() {
                return Ok(Some(Cut::Runaway));
            }
            report(isolate, "uncaughtError", error.message())?;
            loaded = false;
            break;
        }
    }
    if loaded
        && file.kind == Kind::Any
        && let Err(error) = isolate.execute_script("<wpt:done>", "done();")
    {
        if error.is_terminated() {
            return Ok(Some(Cut::Runaway));
        }
        report(isolate, "uncaughtError", error.message())?;
    }

    let mut cut = None;
    let mut phase_deadline = deadline;
    loop {
        let remaining = phase_deadline.saturating_duration_since(Instant::now());
        let step = tokio::time::timeout(remaining, poll_fn(|cx| step(isolate, checkout, cx))).await;
        let reason = match step {
            Ok(Ok(Step::Done)) => return Ok(cut),
            Ok(Ok(Step::Rejection(message))) => {
                report(isolate, "unhandledRejection", &message)?;
                continue;
            }
            Ok(Err(error)) if error.is_terminated() => return Ok(Some(Cut::Runaway)),
            Ok(Err(error)) => return Err(format!("the runner's hooks failed: {error}")),
            Ok(Ok(Step::Stalled)) => Cut::Stalled,
            Err(_elapsed) => Cut::Timeout,
        };
        if cut.is_some() {
            // The harness was timed out already and still has not finished.
            return Ok(cut);
        }
        cut = Some(reason);
        isolate
            .execute_script("<wpt:timeout>", "__wpt_runner.timeout();")
            .map_err(|error| format!("could not time the harness out: {error}"))?;
        phase_deadline = Instant::now() + GRACE;
    }
}

/// Hands an uncaught error or unhandled rejection to the harness.
fn report(isolate: &mut WorkerIsolate, hook: &str, message: &str) -> Result<(), String> {
    let message = message
        .strip_prefix("Uncaught (in promise) ")
        .or_else(|| message.strip_prefix("Uncaught "))
        .unwrap_or(message);
    let message = serde_json::to_string(message).unwrap_or_else(|_| "\"\"".to_string());
    isolate
        .execute_script("<wpt:report>", &format!("__wpt_runner.{hook}({message});"))
        .map(drop)
        .or_else(|error| {
            if error.is_terminated() {
                Ok(())
            } else {
                Err(format!("could not report an error to the harness: {error}"))
            }
        })
}

enum Step {
    Done,
    Stalled,
    Rejection(String),
}

/// Runs the event loop until the harness completes, nothing is left to run,
/// or a promise rejection goes unhandled, serving the data files the test
/// fetches along the way.
fn step(
    isolate: &mut WorkerIsolate,
    checkout: &Checkout,
    cx: &mut std::task::Context<'_>,
) -> Poll<Result<Step, Error>> {
    loop {
        let polled = isolate.poll_event_loop(cx);
        if let Poll::Ready(Err(error)) = &polled
            && error.is_terminated()
        {
            return Poll::Ready(Err(error.clone()));
        }
        let request = match isolate.execute_script("<wpt:poll>", "__wpt_runner.poll()") {
            Ok(request) => request,
            Err(error) => return Poll::Ready(Err(error)),
        };
        if request == "done" {
            return Poll::Ready(Ok(Step::Done));
        }
        if !request.is_empty() {
            if let Err(error) = serve(isolate, checkout, &request) {
                return Poll::Ready(Err(error));
            }
            continue;
        }
        return match polled {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(Step::Stalled)),
            Poll::Ready(Err(error)) => {
                Poll::Ready(Ok(Step::Rejection(error.message().to_string())))
            }
            Poll::Pending => Poll::Pending,
        };
    }
}

/// Answers the test's fetches for data files from the checkout.
fn serve(isolate: &mut WorkerIsolate, checkout: &Checkout, requests: &str) -> Result<(), Error> {
    let requests: Vec<DataRequest> = serde_json::from_str(requests)
        .map_err(|error| Error::new(format!("unreadable data file requests: {error}")))?;
    for request in requests {
        let body = checkout
            .resolve("", &request.path)
            .ok()
            .filter(|path| path.ends_with(".json") || path.ends_with(".idl"))
            .and_then(|path| checkout.read(&path).ok())
            .map_or_else(
                || "null".to_string(),
                |bytes| {
                    format!(
                        "\"{}\"",
                        base64::engine::general_purpose::STANDARD.encode(bytes)
                    )
                },
            );
        isolate.execute_script(
            "<wpt:respond>",
            &format!("__wpt_runner.respond({}, {body});", request.id),
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{Meta, TestFile};
    use std::sync::Arc;

    fn job(path: &str, kind: Kind, variant: &str) -> Job {
        Job {
            file: Arc::new(TestFile {
                path: path.to_string(),
                kind,
                meta: Meta::default(),
                imports: Vec::new(),
            }),
            variant: variant.to_string(),
        }
    }

    #[test]
    fn test_urls_are_the_worker_scripts_wptserve_serves() {
        assert_eq!(
            test_url(&job("streams/piping/abort.any.js", Kind::Any, "")),
            "http://web-platform.test:8000/streams/piping/abort.any.worker.js"
        );
        assert_eq!(
            test_url(&job(
                "WebCryptoAPI/digest/digest.https.any.js",
                Kind::Any,
                "?1-10"
            )),
            "https://web-platform.test:8443/WebCryptoAPI/digest/digest.https.any.worker.js?1-10"
        );
        assert_eq!(
            test_url(&job("FileAPI/FileReaderSync.worker.js", Kind::Worker, "")),
            "http://web-platform.test:8000/FileAPI/FileReaderSync.worker.js"
        );
        assert_eq!(
            origin_of("https://web-platform.test:8443/a/b.js"),
            "https://web-platform.test:8443"
        );
    }

    #[test]
    fn references_resolve_inside_the_checkout() {
        let checkout = Checkout {
            root: PathBuf::from("/wpt"),
            testharness: String::new(),
        };
        assert_eq!(
            checkout.resolve("streams/piping", "../resources/test-utils.js"),
            Ok("streams/resources/test-utils.js".to_string())
        );
        assert_eq!(
            checkout.resolve("streams/piping", "/common/gc.js?x=1"),
            Ok("common/gc.js".to_string())
        );
        assert!(checkout.resolve("streams", "../../etc/passwd").is_err());
    }
}
