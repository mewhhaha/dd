//! The `dd` command line: every subcommand and its flags.
use crate::storage;
use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
#[command(name = "dd")]
#[command(about = "CLI for the dd worker platform")]
pub struct Cli {
    #[arg(long, global = true, value_name = "URL")]
    pub server: Option<String>,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    Storage(storage::StorageCmd),
    Deploy(DeployCmd),
    #[command(hide = true)]
    PackageDeploy(DeployCmd),
    #[command(name = "deploy-config")]
    DeployConfigFile(DeployConfigFileCmd),
    #[command(name = "package-deploy-config", hide = true)]
    PackageDeployConfigFile(DeployConfigFileCmd),
    #[command(name = "auth")]
    Auth(AuthCmd),
    #[command(name = "mint-token", alias = "mint-deploy-token")]
    MintDeployToken(MintDeployTokenCmd),
    #[command(name = "list-tokens")]
    ListTokens,
    #[command(name = "get-token")]
    GetToken(TokenIdCmd),
    #[command(name = "delete-token")]
    DeleteToken(TokenIdCmd),
    #[command(name = "list-deployments")]
    ListDeployments(ListDeploymentsCmd),
    #[command(name = "inspect-deployment")]
    InspectDeployment(DeploymentIdCmd),
    Undeploy(WorkerCmd),
    Rollback(RollbackCmd),
    Invoke(InvokeCmd),
}

#[derive(Args)]
pub struct AuthCmd {
    #[command(subcommand)]
    pub command: AuthCommand,
}

#[derive(Subcommand)]
pub enum AuthCommand {
    Login(AuthLoginCmd),
    Logout,
    Status,
}

#[derive(Args)]
pub struct AuthLoginCmd {
    #[arg(long)]
    pub token: Option<String>,
}

#[derive(Args)]
pub struct DeployCmd {
    pub name: String,

    pub file: String,

    #[arg(long)]
    pub public: bool,

    #[arg(long)]
    pub cache: bool,

    #[arg(long = "assets-dir")]
    pub assets_dir: Option<String>,

    #[arg(long = "server-module", value_name = "TYPE:MODULE_PATH[=FILE]")]
    pub server_modules: Vec<String>,

    #[arg(long = "kv-binding")]
    pub kv_bindings: Vec<String>,

    #[arg(long = "memory-binding")]
    pub memory_bindings: Vec<String>,

    #[arg(long = "service-binding")]
    pub service_bindings: Vec<String>,

    #[arg(long = "trace-worker")]
    pub trace_worker: Option<String>,

    #[arg(long = "trace-path", default_value = "/ingest")]
    pub trace_path: String,

    #[arg(long)]
    pub temporary: bool,
}

#[derive(Args)]
pub struct DeployConfigFileCmd {
    pub config_file: String,

    #[arg(long)]
    pub temporary: bool,

    #[arg(long = "allow-outside-config-root")]
    pub allow_outside_config_root: bool,
}

#[derive(Args)]
pub struct TokenIdCmd {
    pub id: String,
}

#[derive(Args)]
pub struct ListDeploymentsCmd {
    #[arg(long)]
    pub worker: Option<String>,
}

#[derive(Args)]
pub struct DeploymentIdCmd {
    pub id: String,
}

#[derive(Args)]
pub struct WorkerCmd {
    pub worker: String,
}

#[derive(Args)]
pub struct RollbackCmd {
    pub worker: String,
    pub deployment_id: String,
}

#[derive(Args)]
pub struct MintDeployTokenCmd {
    #[arg(long)]
    pub id: Option<String>,

    #[arg(long)]
    pub name: Option<String>,

    #[arg(long = "worker")]
    pub workers: Vec<String>,

    #[arg(long = "any-worker")]
    pub allow_any_worker: bool,

    #[arg(long = "public")]
    pub allow_public: bool,

    #[arg(long = "private")]
    pub allow_private: bool,

    #[arg(long = "kv-binding")]
    pub kv_bindings: Vec<String>,

    #[arg(long = "memory-binding")]
    pub memory_bindings: Vec<String>,

    #[arg(long = "service-binding")]
    pub service_bindings: Vec<String>,

    #[arg(long = "any-binding")]
    pub allow_any_bindings: bool,

    #[arg(long = "allow-internal-trace")]
    pub allow_internal_trace: bool,

    #[arg(long = "max-source-bytes")]
    pub max_source_bytes: Option<u64>,

    #[arg(long = "max-assets")]
    pub max_assets: Option<u64>,

    #[arg(long = "max-asset-bytes")]
    pub max_asset_bytes: Option<u64>,

    #[arg(long = "expires-in-seconds")]
    pub expires_in_seconds: Option<u64>,

    #[arg(long = "expires-at-unix")]
    pub expires_at_unix: Option<u64>,

    #[arg(long = "max-uses")]
    pub max_uses: Option<u64>,
}

#[derive(Args)]
pub struct InvokeCmd {
    pub name: String,

    #[arg(long, default_value = "GET")]
    pub method: String,

    #[arg(long, default_value = "/")]
    pub path: String,

    #[arg(long = "header")]
    pub headers: Vec<String>,

    #[arg(long = "body-file")]
    pub body_file: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::{Cli, Command, DeploymentIdCmd, ListDeploymentsCmd, RollbackCmd, WorkerCmd};
    use clap::Parser;

    #[test]
    fn cli_rejects_retired_memory_binding_flag() {
        let result = Cli::try_parse_from([
            "dd",
            "deploy",
            "worker",
            "examples/hello.js",
            "--actor-binding",
            "ROOMS",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn cli_accepts_temporary_deploy_flag() {
        let cli =
            Cli::try_parse_from(["dd", "deploy", "worker", "examples/hello.js", "--temporary"])
                .expect("parse deploy");
        let Command::Deploy(command) = cli.command else {
            panic!("expected deploy command");
        };
        assert!(command.temporary);
    }

    #[test]
    fn cli_parses_deployment_lifecycle_commands() {
        let cli = Cli::try_parse_from(["dd", "list-deployments", "--worker", "chat"])
            .expect("list deployments");
        assert!(matches!(
            cli.command,
            Command::ListDeployments(ListDeploymentsCmd {
                worker: Some(ref worker)
            }) if worker == "chat"
        ));

        let cli = Cli::try_parse_from(["dd", "inspect-deployment", "deployment-id"])
            .expect("inspect deployment");
        assert!(matches!(
            cli.command,
            Command::InspectDeployment(DeploymentIdCmd { ref id }) if id == "deployment-id"
        ));

        let cli = Cli::try_parse_from(["dd", "undeploy", "chat"]).expect("undeploy");
        assert!(matches!(
            cli.command,
            Command::Undeploy(WorkerCmd { ref worker }) if worker == "chat"
        ));

        let cli =
            Cli::try_parse_from(["dd", "rollback", "chat", "deployment-id"]).expect("rollback");
        assert!(matches!(
            cli.command,
            Command::Rollback(RollbackCmd {
                ref worker,
                ref deployment_id
            }) if worker == "chat" && deployment_id == "deployment-id"
        ));
    }
}
