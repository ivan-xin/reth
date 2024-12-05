//! clap [Args](clap::Args) for Narwhal consensus configuration

use std::time::Duration;
use clap::Args;
use humantime::parse_duration;

/// Parameters for Narwhal consensus configuration
#[derive(Debug, Args, PartialEq, Eq, Default, Clone)]
#[command(next_help_heading = "Narwhal consensus")]
pub struct NarwhalArgs {
    /// Start the node in Narwhal consensus mode
    #[arg(long = "narwhal", help_heading = "Narwhal consensus")]
    pub narwhal: bool,

    /// Start the node in Narwhal dev mode
    ///
    /// This mode uses Narwhal consensus in a local development environment.
    /// Disables network discovery and enables local http server.
    /// Similar to regular dev mode but uses Narwhal consensus instead of PoA.
    #[arg(long = "narwhal.dev", help_heading = "Narwhal consensus")]
    pub narwhal_dev: bool,

    /// How many transactions to include per consensus round
    #[arg(
        long = "narwhal.max-transactions",
        help_heading = "Narwhal consensus",
        conflicts_with = "round_timeout"
    )]
    pub max_transactions: Option<usize>,

    /// Timeout for each consensus round
    ///
    /// Parses strings using [`humantime::parse_duration`]
    /// --narwhal.round-timeout 5s
    #[arg(
        long = "narwhal.round-timeout",
        help_heading = "Narwhal consensus",
        conflicts_with = "max_transactions",
        value_parser = parse_duration,
        verbatim_doc_comment
    )]
    pub round_timeout: Option<Duration>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    /// A helper type to parse Args more easily
    #[derive(Parser)]
    struct CommandParser<T: Args> {
        #[command(flatten)]
        args: T,
    }

    #[test]
    fn test_parse_narwhal_args() {
        let args = CommandParser::<NarwhalArgs>::parse_from(["reth"]).args;
        assert_eq!(args, NarwhalArgs::default());

        let args = CommandParser::<NarwhalArgs>::parse_from(["reth", "--narwhal"]).args;
        assert_eq!(
            args,
            NarwhalArgs {
                narwhal: true,
                narwhal_dev: false,
                max_transactions: None,
                round_timeout: None
            }
        );

        let args = CommandParser::<NarwhalArgs>::parse_from(["reth", "--narwhal.dev"]).args;
        assert_eq!(
            args,
            NarwhalArgs {
                narwhal: false,
                narwhal_dev: true,
                max_transactions: None,
                round_timeout: None
            }
        );

        let args = CommandParser::<NarwhalArgs>::parse_from([
            "reth",
            "--narwhal",
            "--narwhal.max-transactions",
            "100",
        ])
        .args;
        assert_eq!(
            args,
            NarwhalArgs {
                narwhal: true,
                narwhal_dev: false,
                max_transactions: Some(100),
                round_timeout: None
            }
        );

        let args = CommandParser::<NarwhalArgs>::parse_from([
            "reth",
            "--narwhal",
            "--narwhal.round-timeout",
            "5s",
        ])
        .args;
        assert_eq!(
            args,
            NarwhalArgs {
                narwhal: true,
                narwhal_dev: false,
                max_transactions: None,
                round_timeout: Some(Duration::from_secs(5))
            }
        );
    }

    #[test]
    fn test_parse_narwhal_args_conflicts() {
        let args = CommandParser::<NarwhalArgs>::try_parse_from([
            "reth",
            "--narwhal",
            "--narwhal.max-transactions",
            "100",
            "--narwhal.round-timeout",
            "5s",
        ]);
        assert!(args.is_err());
    }

    #[test]
    fn narwhal_args_default_sanity_check() {
        let default_args = NarwhalArgs::default();
        let args = CommandParser::<NarwhalArgs>::parse_from(["reth"]).args;
        assert_eq!(args, default_args);
    }
}
