use clap::{Args, Subcommand};
use std::path::PathBuf;

#[derive(Args)]
pub struct StorageCmd {
    #[command(subcommand)]
    command: StorageCommand,
}

#[derive(Subcommand)]
enum StorageCommand {
    /// Convert an offline store into a new, verified state layout.
    Convert {
        #[arg(long)]
        source: PathBuf,
        #[arg(long)]
        destination: PathBuf,
        #[arg(long)]
        namespace_map: Option<PathBuf>,
    },
}

pub async fn run(command: StorageCmd) -> Result<(), String> {
    match command.command {
        StorageCommand::Convert {
            source,
            destination,
            namespace_map,
        } => {
            let report =
                ::storage::convert::convert(&source, &destination, namespace_map.as_deref())
                    .await
                    .map_err(|error| error.to_string())?;
            println!(
                "{}",
                serde_json::to_string_pretty(&report).map_err(|error| error.to_string())?
            );
            Ok(())
        }
    }
}
