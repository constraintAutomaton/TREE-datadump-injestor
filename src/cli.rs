use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
/// The CLI arguments
pub(crate) struct Cli {
    /// The frequency (output by triple) from which a status report is output to the terminal
    #[arg(short, long, default_value_t = 1_000)]
    pub frequency_notification: usize,

    /// The number of fragment of the outputed TREE document
    #[arg(short, long, default_value_t = 1_000)]
    pub n_fragments: usize,

    /// Path of the configuration file
    /// By default the value is [default: ./config.json]
    #[arg(short, long)]
    pub config_path: Option<PathBuf>,

    /// Path of the output TREE fragmentation
    /// By default the value is [default: ./generated]
    #[arg(short, long)]
    pub output_path: Option<PathBuf>,

    /// Path of the data dump
    /// By default is [default: ../comunica_filter_benchmark/evaluation/data/dahcc_1_participant/data.ttl]
    #[arg(short, long)]
    pub data_dump_path: Option<PathBuf>,

    /// If set to false will put the whole file in memory, with false will read the file line by line
    #[arg(short, long,default_value_t=false, action = clap::ArgAction::SetTrue)]
    pub large_file: bool,
}
