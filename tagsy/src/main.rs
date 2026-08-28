//! Tagsy CLI client.

use std::process::ExitCode;

use clap::Parser;
use tagsy_ipc::IpcBackend;

use crate::commands::Arguments;
use crate::output::{OutputMode, emit_error};
use crate::run::run;

mod commands;
mod common;
mod output;
mod run;
mod upload;

#[tokio::main]
async fn main() -> ExitCode {
    let arguments = Arguments::parse();

    let output_mode = match arguments.json {
        true => OutputMode::Json,
        false => OutputMode::Human,
    };

    let backend = match &arguments.socket {
        Some(path) => IpcBackend::connect(path).await,
        None => IpcBackend::connect_default().await,
    };

    let backend = match backend {
        Ok(backend) => backend,
        Err(error) => {
            emit_error(
                output_mode,
                &format!("failed to connect to the tagsy daemon control socket: {error}"),
            );
            return ExitCode::FAILURE;
        }
    };

    if let Err(error) = run(&backend, arguments.command, output_mode).await {
        emit_error(output_mode, &error);
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}
