//! Runs web-platform-tests against dd's worker isolate.
//!
//! Every test file runs in a fresh, production-configured worker isolate:
//! testharness.js, the file's `// META: script=` includes, then the file,
//! the way wptserve's classic worker wrapper loads it. Results are compared
//! with `wpt/expectations.json`; any difference, an unexpected pass
//! included, fails the run. `--update` rewrites the expectations instead.
//!
//! The tests come from the checkout `scripts/wpt/fetch.sh` makes of the
//! commit pinned in `wpt/WPT_SHA`; `wpt/config.json` lists the areas to run
//! and the files to skip, each with its reason. See docs/development.md.

mod expectations;
mod manifest;
mod run;

use expectations::Unexpected;
use manifest::{Job, TestFile};
use run::{Checkout, HarnessStatus, Outcome, SubtestStatus};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

const USAGE: &str = "\
Usage: wpt [OPTIONS] [PATH...]

Runs the web-platform-tests in wpt/config.json against dd's worker isolate
and compares the results with wpt/expectations.json.

Arguments:
  [PATH...]                  Only run tests whose path starts with PATH,
                             e.g. streams/piping or url/url-constructor.any.js

Options:
  --update                   Rewrite the expectations with this run's results
  --jobs <N>                 Isolates to run at once [default: available cores]
  --timeout-multiplier <X>   Scale the per-file timeouts (10s, 60s for
                             `timeout=long`) [default: 3 in debug builds,
                             as wptrunner does, else 1]
  --wpt-dir <DIR>            The WPT checkout [default: $WPT_DIR or .cache/wpt]
  -v, --verbose              Print every file's result and failing subtests
  -h, --help                 Print this help
";

const TIMEOUT: Duration = Duration::from_secs(10);
const LONG_TIMEOUT: Duration = Duration::from_secs(60);
/// Unoptimized builds run Web Crypto's key generation several times slower.
const DEFAULT_TIMEOUT_MULTIPLIER: f64 = if cfg!(debug_assertions) { 3.0 } else { 1.0 };

struct Args {
    update: bool,
    jobs: usize,
    timeout_multiplier: f64,
    wpt_dir: Option<PathBuf>,
    verbose: bool,
    filters: Vec<String>,
}

fn parse_args() -> Result<Option<Args>, String> {
    let mut args = Args {
        update: false,
        jobs: std::thread::available_parallelism().map_or(4, usize::from),
        timeout_multiplier: DEFAULT_TIMEOUT_MULTIPLIER,
        wpt_dir: None,
        verbose: false,
        filters: Vec::new(),
    };
    let mut raw = std::env::args().skip(1);
    while let Some(arg) = raw.next() {
        let mut value = |name: &str| raw.next().ok_or_else(|| format!("{name} needs a value"));
        match arg.as_str() {
            "-h" | "--help" => return Ok(None),
            "--update" => args.update = true,
            "-v" | "--verbose" => args.verbose = true,
            "--jobs" => {
                args.jobs = value("--jobs")?
                    .parse()
                    .ok()
                    .filter(|jobs| *jobs > 0)
                    .ok_or("--jobs needs a positive number")?;
            }
            "--timeout-multiplier" => {
                args.timeout_multiplier = value("--timeout-multiplier")?
                    .parse()
                    .ok()
                    .filter(|multiplier: &f64| *multiplier > 0.0)
                    .ok_or("--timeout-multiplier needs a positive number")?;
            }
            "--wpt-dir" => args.wpt_dir = Some(PathBuf::from(value("--wpt-dir")?)),
            flag if flag.starts_with('-') => {
                return Err(format!("unknown option {flag}\n\n{USAGE}"));
            }
            filter => args.filters.push(filter.trim_matches('/').to_string()),
        }
    }
    Ok(Some(args))
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    /// The directories of the checkout to run, relative to its root.
    areas: Vec<String>,
    /// Files (or directories, ending in `/`) not to run, with the reason.
    skip: BTreeMap<String, String>,
}

fn main() -> ExitCode {
    match run_suite() {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(message) => {
            eprintln!("wpt: {message}");
            ExitCode::from(2)
        }
    }
}

fn run_suite() -> Result<bool, String> {
    let Some(args) = parse_args()? else {
        print!("{USAGE}");
        return Ok(true);
    };
    let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let repo = repo.canonicalize().unwrap_or(repo);
    let wpt_dir = args
        .wpt_dir
        .clone()
        .or_else(|| std::env::var_os("WPT_DIR").map(PathBuf::from))
        .unwrap_or_else(|| repo.join(".cache/wpt"));
    check_pin(&repo.join("wpt/WPT_SHA"), &wpt_dir)?;
    let config_path = repo.join("wpt/config.json");
    let config: Config = serde_json::from_str(
        &fs::read_to_string(&config_path)
            .map_err(|error| format!("failed to read {}: {error}", config_path.display()))?,
    )
    .map_err(|error| format!("{} is not valid: {error}", config_path.display()))?;
    let expectations_path = repo.join("wpt/expectations.json");
    let mut expectations = expectations::load(&expectations_path)?;
    let checkout = Checkout::open(&wpt_dir)?;

    let in_scope = |path: &str| {
        args.filters.is_empty()
            || args.filters.iter().any(|filter| {
                path == filter
                    || path
                        .strip_prefix(filter.as_str())
                        .is_some_and(|rest| rest.starts_with('/'))
            })
    };
    let mut jobs = Vec::new();
    let mut skipped = BTreeMap::new();
    let mut used_skips = BTreeSet::new();
    // An area is worth listing when a filter covers it or points inside it.
    let overlaps = |area: &str| {
        in_scope(area)
            || args.filters.iter().any(|filter| {
                filter
                    .strip_prefix(area)
                    .is_some_and(|rest| rest.starts_with('/'))
            })
    };
    for area in config.areas.iter().filter(|area| overlaps(area)) {
        for file in manifest::discover(&wpt_dir, area)? {
            if !in_scope(&file.path) {
                continue;
            }
            let file = Arc::new(file);
            let variants = if file.meta.variants.is_empty() {
                vec![String::new()]
            } else {
                file.meta.variants.clone()
            };
            for variant in variants {
                let job = Job {
                    file: Arc::clone(&file),
                    variant,
                };
                match skip_reason(&config.skip, &job) {
                    Some((key, reason)) => {
                        used_skips.insert(key.to_string());
                        skipped.insert(job.id(), reason.to_string());
                    }
                    None => jobs.push(job),
                }
            }
        }
    }
    if jobs.is_empty() {
        return Err("no tests matched".to_string());
    }
    // Long tests first, so they do not trail at the end.
    jobs.sort_by_key(|job| (!job.file.meta.long_timeout, job.id()));

    let started = Instant::now();
    warm_up()?;
    let outcomes = run_jobs(&jobs, &checkout, &args);
    let elapsed = started.elapsed();

    let mut summary = Summary::default();
    let mut ran = BTreeSet::new();
    let mut observed_all = BTreeMap::new();
    for (job, outcome) in jobs.iter().zip(&outcomes) {
        let id = job.id();
        let observed = expectations::observed(outcome);
        let unexpected = expectations::compare(&observed, expectations.get(&id));
        summary.add(area_of(&config.areas, &job.file), outcome, unexpected.len());
        if !unexpected.is_empty() {
            summary.differences.push(Difference {
                id: id.clone(),
                note: outcome_note(outcome),
                unexpected,
                messages: messages(outcome),
            });
        }
        ran.insert(id.clone());
        observed_all.insert(id, observed.to_expectations());
    }
    let stale: Vec<String> = expectations
        .keys()
        .filter(|id| in_scope(id.split('?').next().unwrap_or(id)) && !ran.contains(*id))
        .cloned()
        .collect();

    summary.print(&args, elapsed, &outcomes, &jobs, &skipped);
    if args.filters.is_empty() {
        for key in config.skip.keys().filter(|key| !used_skips.contains(*key)) {
            println!("warning: the skip entry {key} matches no test");
        }
    }

    if args.update {
        for id in &stale {
            expectations.remove(id);
        }
        expectations.extend(observed_all);
        expectations::save(&expectations_path, &expectations)?;
        println!(
            "\nUpdated {} ({} files changed or added, {} removed).",
            expectations_path.display(),
            summary.differences.len(),
            stale.len()
        );
        return Ok(true);
    }

    for id in &stale {
        println!("STALE {id}: has expectations but did not run");
    }
    let clean = summary.differences.is_empty() && stale.is_empty();
    if !clean {
        println!(
            "\nResults differ from {}. If the change is intended, run `just wpt --update` and commit the result.",
            expectations_path.display()
        );
    }
    Ok(clean)
}

/// Refuses a checkout that is not at the pinned commit, so the pin is the
/// only thing that decides the test inputs.
fn check_pin(pin_path: &Path, wpt_dir: &Path) -> Result<(), String> {
    let pin = fs::read_to_string(pin_path)
        .map_err(|error| format!("failed to read {}: {error}", pin_path.display()))?;
    let pin = pin.trim();
    let head_path = wpt_dir.join(".git/HEAD");
    let head = fs::read_to_string(&head_path).map_err(|_| {
        format!(
            "no WPT checkout at {}; run scripts/wpt/fetch.sh (or `just wpt`)",
            wpt_dir.display()
        )
    })?;
    let head = head.trim();
    let head = match head.strip_prefix("ref: ") {
        Some(reference) => fs::read_to_string(wpt_dir.join(".git").join(reference))
            .map(|sha| sha.trim().to_string())
            .unwrap_or_default(),
        None => head.to_string(),
    };
    if head != pin {
        return Err(format!(
            "the WPT checkout at {} is at {}, but wpt/WPT_SHA pins {pin}; run scripts/wpt/fetch.sh",
            wpt_dir.display(),
            if head.is_empty() {
                "an unknown commit"
            } else {
                &head
            }
        ));
    }
    Ok(())
}

/// The skip entry that covers `job`: its file, one of its variants (`path?query`),
/// a directory (`dir/`), or a path prefix (`dir/name-*`).
fn skip_reason<'a>(skip: &'a BTreeMap<String, String>, job: &Job) -> Option<(&'a str, &'a str)> {
    let id = job.id();
    skip.iter()
        .find(|(key, _)| {
            let prefix = key.strip_suffix('*').unwrap_or(key);
            **key == id
                || **key == job.file.path
                || (key.ends_with('/') || key.ends_with('*')) && job.file.path.starts_with(prefix)
        })
        .map(|(key, reason)| (key.as_str(), reason.as_str()))
}

fn area_of<'a>(areas: &'a [String], file: &TestFile) -> &'a str {
    areas
        .iter()
        .filter(|area| file.path.starts_with(&format!("{area}/")))
        .max_by_key(|area| area.len())
        .map_or("", String::as_str)
}

/// Builds the bootstrap snapshot once, before the workers race to.
fn warm_up() -> Result<(), String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("failed to start tokio: {error}"))?;
    runtime
        .block_on(runtime::conformance::WorkerIsolate::new())
        .map(drop)
        .map_err(|error| format!("could not create a worker isolate: {error}"))
}

fn run_jobs(jobs: &[Job], checkout: &Checkout, args: &Args) -> Vec<Outcome> {
    let next = AtomicUsize::new(0);
    let (sender, receiver) = mpsc::channel();
    let mut outcomes: Vec<Option<Outcome>> = jobs.iter().map(|_| None).collect();
    std::thread::scope(|scope| {
        for worker in 0..args.jobs.min(jobs.len()) {
            let sender = sender.clone();
            let next = &next;
            std::thread::Builder::new()
                .name(format!("wpt-{worker}"))
                .stack_size(8 * 1024 * 1024)
                .spawn_scoped(scope, move || {
                    loop {
                        let index = next.fetch_add(1, Ordering::Relaxed);
                        let Some(job) = jobs.get(index) else {
                            break;
                        };
                        let timeout = if job.file.meta.long_timeout {
                            LONG_TIMEOUT
                        } else {
                            TIMEOUT
                        }
                        .mul_f64(args.timeout_multiplier);
                        // A runtime per file: blocking work a timed-out test
                        // left behind (key generation, say) is abandoned
                        // with it instead of holding up the next file.
                        let runtime = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .expect("tokio runtime should start");
                        let outcome = runtime.block_on(run::run_job(job, checkout, timeout));
                        runtime.shutdown_background();
                        if sender.send((index, outcome)).is_err() {
                            break;
                        }
                    }
                })
                .expect("worker thread should start");
        }
        drop(sender);
        let mut done = 0;
        for (index, outcome) in receiver {
            done += 1;
            if args.verbose {
                println!(
                    "[{done}/{}] {} {} ({:.1}s)",
                    jobs.len(),
                    jobs[index].id(),
                    outcome_note(&outcome),
                    outcome.duration.as_secs_f64()
                );
                for (name, message) in messages(&outcome) {
                    println!("  {name}: {message}");
                }
            } else if done % 100 == 0 {
                println!("[{done}/{}]", jobs.len());
            }
            outcomes[index] = Some(outcome);
        }
    });
    outcomes
        .into_iter()
        .map(|outcome| outcome.expect("every job reports an outcome"))
        .collect()
}

fn outcome_note(outcome: &Outcome) -> String {
    let passed = outcome
        .subtests
        .iter()
        .filter(|subtest| subtest.status == SubtestStatus::Pass)
        .count();
    let mut note = format!("{passed}/{} subtests pass", outcome.subtests.len());
    if outcome.harness != HarnessStatus::Ok {
        note.push_str(&format!(", harness {}", outcome.harness.label()));
        if let Some(message) = &outcome.harness_message {
            note.push_str(&format!(": {}", first_line(message)));
        }
    }
    note
}

fn messages(outcome: &Outcome) -> BTreeMap<String, String> {
    let mut messages = BTreeMap::new();
    for subtest in &outcome.subtests {
        if subtest.status != SubtestStatus::Pass {
            let message = subtest
                .message
                .as_deref()
                .map(first_line)
                .unwrap_or_default();
            messages.insert(
                subtest.name.clone(),
                format!("{} {}", subtest.status.label(), message)
                    .trim_end()
                    .to_string(),
            );
        }
    }
    if outcome.harness != HarnessStatus::Ok {
        let message = outcome
            .harness_message
            .as_deref()
            .map(first_line)
            .unwrap_or_default();
        messages.insert(
            expectations::HARNESS_KEY.to_string(),
            format!("{} {}", outcome.harness.label(), message)
                .trim_end()
                .to_string(),
        );
    }
    messages
}

fn first_line(text: &str) -> &str {
    text.lines().next().unwrap_or_default()
}

#[derive(Default)]
struct AreaSummary {
    runs: usize,
    pass: usize,
    fail: usize,
    harness_errors: usize,
    timeouts: usize,
    unexpected: usize,
}

/// A file whose results differ from its expectations.
struct Difference {
    id: String,
    note: String,
    unexpected: Vec<Unexpected>,
    /// Status and message of each subtest that did not pass.
    messages: BTreeMap<String, String>,
}

#[derive(Default)]
struct Summary {
    areas: BTreeMap<String, AreaSummary>,
    differences: Vec<Difference>,
}

impl Summary {
    fn add(&mut self, area: &str, outcome: &Outcome, unexpected: usize) {
        let summary = self.areas.entry(area.to_string()).or_default();
        summary.runs += 1;
        for subtest in &outcome.subtests {
            if subtest.status == SubtestStatus::Pass {
                summary.pass += 1;
            } else {
                summary.fail += 1;
            }
        }
        match outcome.harness {
            HarnessStatus::Ok => {}
            HarnessStatus::Timeout => summary.timeouts += 1,
            HarnessStatus::Error | HarnessStatus::PreconditionFailed => summary.harness_errors += 1,
        }
        summary.unexpected += unexpected;
    }

    fn print(
        &self,
        args: &Args,
        elapsed: Duration,
        outcomes: &[Outcome],
        jobs: &[Job],
        skipped: &BTreeMap<String, String>,
    ) {
        if args.verbose && !skipped.is_empty() {
            println!("\nSkipped:");
            for (id, reason) in skipped {
                println!("  {id}: {reason}");
            }
        }
        if !args.update && !self.differences.is_empty() {
            println!();
            for difference in &self.differences {
                println!("{} ({})", difference.id, difference.note);
                for unexpected in &difference.unexpected {
                    let message = unexpected
                        .subtest()
                        .and_then(|name| difference.messages.get(name));
                    match message {
                        Some(message) => println!("  {} ({message})", unexpected.describe()),
                        None => println!("  {}", unexpected.describe()),
                    }
                }
            }
        }

        println!(
            "\n{:<36} {:>6} {:>8} {:>7} {:>7} {:>7} {:>8} {:>10}",
            "area", "runs", "subtests", "pass", "fail", "errors", "timeouts", "unexpected"
        );
        let mut total = AreaSummary::default();
        for (area, summary) in &self.areas {
            print_area_row(area, summary);
            total.runs += summary.runs;
            total.pass += summary.pass;
            total.fail += summary.fail;
            total.harness_errors += summary.harness_errors;
            total.timeouts += summary.timeouts;
            total.unexpected += summary.unexpected;
        }
        print_area_row("total", &total);

        let mut slowest: Vec<(Duration, String)> = outcomes
            .iter()
            .zip(jobs)
            .map(|(outcome, job)| (outcome.duration, job.id()))
            .collect();
        slowest.sort_by_key(|(duration, _)| std::cmp::Reverse(*duration));
        println!("\nSlowest runs:");
        for (duration, id) in slowest.iter().take(5) {
            println!("  {:>6.1}s {id}", duration.as_secs_f64());
        }
        println!(
            "\nRan {} test files (once per variant) in {:.1}s with {} jobs; skipped {}.",
            jobs.len(),
            elapsed.as_secs_f64(),
            args.jobs,
            skipped.len()
        );
    }
}

fn print_area_row(area: &str, summary: &AreaSummary) {
    println!(
        "{:<36} {:>6} {:>8} {:>7} {:>7} {:>7} {:>8} {:>10}",
        area,
        summary.runs,
        summary.pass + summary.fail,
        summary.pass,
        summary.fail,
        summary.harness_errors,
        summary.timeouts,
        summary.unexpected
    );
}
