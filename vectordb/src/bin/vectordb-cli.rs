use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "vectordb-cli")]
#[command(about = "VectorDB command line utility")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Version,
}

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Command::Version => {
            println!("{}", vectordb::VERSION);
        }
    }
}
