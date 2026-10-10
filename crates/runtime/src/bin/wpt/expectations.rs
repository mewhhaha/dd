//! The checked-in expectations: for every test file run (and variant), how
//! many subtests it has, how many pass, and which do not.
//!
//! ```json
//! {
//!   "FileAPI/FileReaderSync.worker.js": { "harness": "fail", "subtests": 0, "pass": 0 },
//!   "streams/piping/abort.any.js": { "subtests": 42, "pass": 41, "fail": [
//!     "pipeTo on a teed readable byte stream should only be aborted when both branches are aborted"
//!   ] },
//!   "url/url-statics-parse.any.js": { "subtests": 12, "pass": 12 }
//! }
//! ```
//!
//! A subtest passes when testharness.js reports PASS; any other status is a
//! failure. `fail` lists the subtests that do not pass, and is left out when
//! none does (`pass` is 0) or all do. `harness` is there only when the
//! harness itself did not finish OK (an uncaught error, a timeout).
//!
//! Against a run, any difference is unexpected: a subtest that fails without
//! being listed, a listed one that passes or no longer exists, and a change
//! in either count (a subtest appeared or disappeared).

use crate::run::{HarnessStatus, Outcome, SubtestStatus};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

/// How a harness that did not finish OK is named in reports.
pub const HARNESS_KEY: &str = "(harness)";

pub type Expectations = BTreeMap<String, FileExpectations>;

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileExpectations {
    #[serde(default)]
    harness: Option<HarnessExpectation>,
    subtests: usize,
    pass: usize,
    #[serde(default)]
    fail: Option<BTreeSet<String>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
enum HarnessExpectation {
    Fail,
}

impl FileExpectations {
    /// Whether `name` is expected to fail.
    fn fails(&self, name: &str) -> bool {
        match &self.fail {
            Some(fail) => fail.contains(name),
            None => self.pass == 0,
        }
    }
}

/// One run's results: whether the harness finished OK, and whether each
/// subtest passed.
pub struct Observed {
    harness_ok: bool,
    subtests: BTreeMap<String, bool>,
}

impl Observed {
    fn passes(&self) -> usize {
        self.subtests.values().filter(|passed| **passed).count()
    }

    /// The expectations that this run would meet exactly.
    pub fn to_expectations(&self) -> FileExpectations {
        let pass = self.passes();
        let fail: BTreeSet<String> = self
            .subtests
            .iter()
            .filter(|(_, passed)| !**passed)
            .map(|(name, _)| name.clone())
            .collect();
        FileExpectations {
            harness: (!self.harness_ok).then_some(HarnessExpectation::Fail),
            subtests: self.subtests.len(),
            pass,
            fail: (pass > 0 && !fail.is_empty()).then_some(fail),
        }
    }
}

pub fn observed(outcome: &Outcome) -> Observed {
    let mut subtests = BTreeMap::new();
    for subtest in &outcome.subtests {
        let passed = subtest.status == SubtestStatus::Pass;
        // Repeated names count as one subtest, failing if any run failed.
        let entry = subtests.entry(subtest.name.clone()).or_insert(passed);
        *entry &= passed;
    }
    Observed {
        harness_ok: outcome.harness == HarnessStatus::Ok,
        subtests,
    }
}

pub fn load(path: &Path) -> Result<Expectations, String> {
    match fs::read_to_string(path) {
        Ok(text) => serde_json::from_str(&text)
            .map_err(|error| format!("{} is not valid: {error}", path.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Expectations::new()),
        Err(error) => Err(format!("failed to read {}: {error}", path.display())),
    }
}

/// Writes one line per file, plus one per failing subtest, in key order, so
/// a change in results changes only the lines it concerns.
pub fn save(path: &Path, expectations: &Expectations) -> Result<(), String> {
    fs::write(path, render(expectations))
        .map_err(|error| format!("failed to write {}: {error}", path.display()))
}

fn render(expectations: &Expectations) -> String {
    let quote = |text: &str| serde_json::to_string(text).expect("strings serialize");
    let mut out = String::from("{\n");
    let mut files = expectations.iter().peekable();
    while let Some((id, file)) = files.next() {
        out.push_str(&format!("  {}: {{ ", quote(id)));
        if file.harness.is_some() {
            out.push_str("\"harness\": \"fail\", ");
        }
        out.push_str(&format!(
            "\"subtests\": {}, \"pass\": {}",
            file.subtests, file.pass
        ));
        match &file.fail {
            Some(fail) => {
                out.push_str(", \"fail\": [\n");
                let mut names = fail.iter().peekable();
                while let Some(name) = names.next() {
                    out.push_str("    ");
                    out.push_str(&quote(name));
                    out.push_str(if names.peek().is_some() { ",\n" } else { "\n" });
                }
                out.push_str("  ] }");
            }
            None => out.push_str(" }"),
        }
        out.push_str(if files.peek().is_some() { ",\n" } else { "\n" });
    }
    out.push_str("}\n");
    out
}

/// A difference between a run and its expectations.
#[derive(Debug, PartialEq, Eq)]
pub enum Unexpected {
    /// The file has no expectations.
    Unrecorded,
    /// Failed without being listed as failing.
    Fail(String),
    /// Listed as failing, but passed.
    Pass(String),
    /// Listed as failing, but not reported at all.
    Stale(String),
    /// The number of subtests, or of passes, changed beyond the subtests
    /// named above: a subtest appeared or disappeared.
    Counts {
        expected: (usize, usize),
        observed: (usize, usize),
    },
}

impl Unexpected {
    /// The subtest the difference is about, if it is about one.
    pub fn subtest(&self) -> Option<&str> {
        match self {
            Self::Fail(name) | Self::Pass(name) | Self::Stale(name) => Some(name),
            Self::Unrecorded | Self::Counts { .. } => None,
        }
    }

    pub fn describe(&self) -> String {
        match self {
            Self::Unrecorded => "NEW: the file has no expectations".to_string(),
            Self::Fail(name) => format!("UNEXPECTED FAIL: {name}"),
            Self::Pass(name) => format!("UNEXPECTED PASS: {name}"),
            Self::Stale(name) => format!("STALE: {name} is listed as failing but did not run"),
            Self::Counts { expected, observed } => format!(
                "COUNTS: {} subtests with {} passing, expected {} with {}",
                observed.0, observed.1, expected.0, expected.1
            ),
        }
    }
}

/// Compares a run of one file with its expectations.
pub fn compare(observed: &Observed, expected: Option<&FileExpectations>) -> Vec<Unexpected> {
    let Some(expected) = expected else {
        return vec![Unexpected::Unrecorded];
    };
    let mut unexpected = Vec::new();
    match (expected.harness.is_some(), observed.harness_ok) {
        (false, false) => unexpected.push(Unexpected::Fail(HARNESS_KEY.to_string())),
        (true, true) => unexpected.push(Unexpected::Pass(HARNESS_KEY.to_string())),
        _ => {}
    }
    let mut named_pass_delta: isize = 0;
    for (name, passed) in &observed.subtests {
        match (*passed, expected.fails(name)) {
            (true, true) => {
                unexpected.push(Unexpected::Pass(name.clone()));
                named_pass_delta += 1;
            }
            (false, false) => {
                unexpected.push(Unexpected::Fail(name.clone()));
                named_pass_delta -= 1;
            }
            _ => {}
        }
    }
    for name in expected.fail.iter().flatten() {
        if !observed.subtests.contains_key(name) {
            unexpected.push(Unexpected::Stale(name.clone()));
        }
    }
    let observed_counts = (observed.subtests.len(), observed.passes());
    let explained_passes = expected.pass as isize + named_pass_delta;
    if observed_counts.0 != expected.subtests || observed_counts.1 as isize != explained_passes {
        unexpected.push(Unexpected::Counts {
            expected: (expected.subtests, expected.pass),
            observed: observed_counts,
        });
    }
    unexpected
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(harness_ok: bool, subtests: &[(&str, bool)]) -> Observed {
        Observed {
            harness_ok,
            subtests: subtests
                .iter()
                .map(|(name, passed)| (name.to_string(), *passed))
                .collect(),
        }
    }

    fn described(unexpected: Vec<Unexpected>) -> Vec<String> {
        unexpected.iter().map(Unexpected::describe).collect()
    }

    #[test]
    fn a_run_meets_the_expectations_it_records() {
        for observed in [
            run(true, &[("a", true), ("b", false)]),
            run(false, &[]),
            run(true, &[("a", false), ("b", false)]),
            run(true, &[("a", true)]),
        ] {
            let expected = observed.to_expectations();
            assert!(compare(&observed, Some(&expected)).is_empty());
        }
    }

    #[test]
    fn differences_in_either_direction_are_unexpected() {
        let expected = run(
            true,
            &[("a", true), ("b", false), ("c", true), ("gone", false)],
        )
        .to_expectations();
        let observed = run(
            false,
            &[("a", false), ("b", true), ("c", true), ("new", true)],
        );
        assert_eq!(
            described(compare(&observed, Some(&expected))),
            [
                "UNEXPECTED FAIL: (harness)",
                "UNEXPECTED FAIL: a",
                "UNEXPECTED PASS: b",
                "STALE: gone is listed as failing but did not run",
                "COUNTS: 4 subtests with 3 passing, expected 4 with 2",
            ]
        );
    }

    #[test]
    fn a_vanished_passing_subtest_changes_the_counts() {
        let expected = run(true, &[("a", true), ("b", true), ("c", false)]).to_expectations();
        let observed = run(true, &[("a", true), ("c", false)]);
        assert_eq!(
            described(compare(&observed, Some(&expected))),
            ["COUNTS: 2 subtests with 1 passing, expected 3 with 2"]
        );
    }

    #[test]
    fn a_file_where_nothing_passes_lists_no_names() {
        let expected = run(true, &[("a", false), ("b", false)]).to_expectations();
        assert_eq!(expected.fail, None);
        let observed = run(true, &[("a", true), ("b", false), ("c", false)]);
        assert_eq!(
            described(compare(&observed, Some(&expected))),
            [
                "UNEXPECTED PASS: a",
                "COUNTS: 3 subtests with 1 passing, expected 2 with 0",
            ]
        );
    }

    #[test]
    fn rendering_is_json_that_reads_back() {
        let expectations: Expectations = [
            ("x.any.js".to_string(), run(false, &[]).to_expectations()),
            (
                "y.any.js?1-10".to_string(),
                run(true, &[("q\"uote", false), ("ok", true), ("tab\t", false)]).to_expectations(),
            ),
            (
                "z.any.js".to_string(),
                run(true, &[("ok", true)]).to_expectations(),
            ),
        ]
        .into_iter()
        .collect();
        let text = render(&expectations);
        assert_eq!(
            text,
            "{\n  \"x.any.js\": { \"harness\": \"fail\", \"subtests\": 0, \"pass\": 0 },\n  \"y.any.js?1-10\": { \"subtests\": 3, \"pass\": 1, \"fail\": [\n    \"q\\\"uote\",\n    \"tab\\t\"\n  ] },\n  \"z.any.js\": { \"subtests\": 1, \"pass\": 1 }\n}\n"
        );
        let read: Expectations = serde_json::from_str(&text).expect("rendered JSON parses");
        assert_eq!(read, expectations);
    }
}
