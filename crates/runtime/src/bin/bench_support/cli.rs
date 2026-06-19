#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum BenchArgAction {
    Run,
    Help,
}

pub(crate) fn bench_arg_action<I, S>(args: I) -> Result<BenchArgAction, String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    for arg in args {
        let arg = arg.as_ref();
        match arg {
            "-h" | "--help" => return Ok(BenchArgAction::Help),
            value => {
                return Err(format!(
                    "unsupported argument `{value}`; configure this benchmark with DD_BENCH_* env vars or run with --help"
                ));
            }
        }
    }
    Ok(BenchArgAction::Run)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bench_arg_action_handles_help() {
        assert_eq!(
            bench_arg_action(["--help"]).expect("help should parse"),
            BenchArgAction::Help
        );
        assert_eq!(
            bench_arg_action(["-h"]).expect("help should parse"),
            BenchArgAction::Help
        );
    }

    #[test]
    fn bench_arg_action_runs_without_args() {
        assert_eq!(
            bench_arg_action(std::iter::empty::<&str>()).expect("empty args should parse"),
            BenchArgAction::Run
        );
    }

    #[test]
    fn bench_arg_action_rejects_unknown_args() {
        let error = bench_arg_action(["--requests", "10"]).expect_err("arg should fail");
        assert!(error.contains("unsupported argument"));
    }
}
