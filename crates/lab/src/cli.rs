use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

use crate::runtime::{
    clean_lab, configure_trust, manage_node, manage_relay, print_env, print_status, run_manager,
    start_background_lab, stop_selected_lab,
};
use crate::state::{NodeCommand, ProcessCommand, TrustMode};
use crate::util::selected_root;

#[derive(Parser, Debug)]
#[command(
    version,
    about = "Create and manage a local Fungi relay + two-node lab"
)]
pub struct LabCli {
    /// Lab data directory (not a source checkout or a node's fungi-dir).
    #[arg(
        long = "lab-dir",
        value_name = "PATH",
        global = true,
        env = "FUNGI_LAB_DIR"
    )]
    root: Option<PathBuf>,
    #[command(subcommand)]
    command: LabCommand,
}

impl LabCli {
    pub fn run(self) -> Result<()> {
        let root = selected_root(self.root)?;
        let _lock = match &self.command {
            LabCommand::Manager(_) | LabCommand::Status(_) | LabCommand::Env => None,
            LabCommand::Start(_) => Some(crate::util::lock_lab(&root, true)?),
            _ => Some(crate::util::lock_lab(&root, false)?),
        };
        match self.command {
            LabCommand::Start(mut args) => {
                args.root = Some(root);
                start_background_lab(args)
            }
            LabCommand::Status(args) => print_status(&root, args),
            LabCommand::Stop => stop_selected_lab(&root),
            LabCommand::Clean => clean_lab(&root),
            LabCommand::Env => print_env(&root),
            LabCommand::Node { command } => manage_node(&root, command),
            LabCommand::Relay { command } => manage_relay(&root, command),
            LabCommand::Trust { mode } => configure_trust(&root, mode),
            LabCommand::Manager(mut args) => {
                args.root = root;
                run_manager(args)
            }
        }
    }
}

#[derive(Subcommand, Debug)]
pub(crate) enum LabCommand {
    /// Start a background local relay + node-a + node-b lab.
    Start(StartArgs),
    /// Show the current local lab state.
    Status(StatusArgs),
    /// Stop lab processes but keep node directories and logs.
    Stop,
    /// Stop lab processes and remove the owned lab data directory.
    Clean,
    /// Print shell exports for the current lab.
    Env,
    /// Stop, start, or restart one lab node.
    Node {
        #[command(subcommand)]
        command: NodeCommand,
    },
    /// Stop, start, or restart the local relay.
    Relay {
        #[command(subcommand)]
        command: ProcessCommand,
    },
    /// Reconfigure trusted-device direction between node-a and node-b.
    Trust {
        #[arg(value_enum)]
        mode: TrustMode,
    },
    #[command(hide = true)]
    Manager(ManagerArgs),
}

#[derive(Parser, Debug)]
pub(crate) struct StartArgs {
    /// Path to the fungi binary. Defaults to target/debug/fungi next to this binary.
    #[arg(long = "fungi-bin")]
    pub(crate) fungi_bin: Option<PathBuf>,
    #[arg(skip)]
    pub(crate) root: Option<PathBuf>,
    /// Trusted-device direction to configure after startup.
    #[arg(long, value_enum, default_value_t = TrustMode::None)]
    pub(crate) trust: TrustMode,
}

#[derive(Parser, Debug)]
pub(crate) struct StatusArgs {
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Parser, Debug, Clone)]
pub(crate) struct ManagerArgs {
    #[arg(long)]
    pub(crate) repo: PathBuf,
    #[arg(long = "fungi-bin")]
    pub(crate) fungi_bin: PathBuf,
    #[arg(skip)]
    pub(crate) root: PathBuf,
    #[arg(long, value_enum)]
    pub(crate) trust: TrustMode,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn start_defaults_to_no_trust() {
        let cli = LabCli::try_parse_from(["fungi-lab", "start"]).unwrap();
        let LabCommand::Start(args) = cli.command else {
            panic!("expected start command");
        };
        assert_eq!(args.trust, TrustMode::None);
    }

    #[test]
    fn root_is_a_global_selector() {
        for args in [
            vec!["fungi-lab", "--lab-dir", "/tmp/lab", "status"],
            vec!["fungi-lab", "status", "--lab-dir", "/tmp/lab"],
        ] {
            let cli = LabCli::try_parse_from(args).unwrap();
            assert_eq!(cli.root, Some(PathBuf::from("/tmp/lab")));
        }
    }
}
