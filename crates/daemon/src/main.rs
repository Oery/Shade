mod error;

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter::LevelFilter;

use crate::error::Error;

/// Shade daemon. Use the `shade` client to talk to it.
#[derive(Parser)]
#[command(name = "shaded", version)]
struct Args {
    /// Configuration file. Defaults to the XDG config location.
    #[arg(long)]
    config: Option<PathBuf>,
}

fn main() -> ExitCode {
    let args = Args::parse();
    match run(args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: Args) -> Result<(), Error> {
    init_tracing()?;
    shade_core::run(args.config)?;
    Ok(())
}

fn init_tracing() -> Result<(), Error> {
    let filter = EnvFilter::builder().with_default_directive(LevelFilter::INFO.into()).from_env_lossy();
    let subscriber = tracing_subscriber::fmt().with_env_filter(filter).with_writer(std::io::stderr);
    if let Err(source) = subscriber.try_init() {
        return Err(Error::Tracing(source));
    }
    Ok(())
}
