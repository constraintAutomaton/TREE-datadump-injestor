mod cli;
mod config;
mod fragment;
mod member;
mod read_datadump;
mod tree;

use clap::Parser;
use cli::*;
use config::*;
use humantime::format_duration;
use read_datadump::*;
use std::path::PathBuf;
use std::time;
use tokio;

#[tokio::main]
async fn main() {
    let start = time::Instant::now();
    // will be CLI param
    let cli = Cli::parse();
    let config_path = cli.config_path.unwrap_or(PathBuf::from("./config.json"));
    let data_injection_config = Config::new(config_path);
    let notice_frequency = cli.frequency_notification;
    let n_fragments = cli.n_fragments;
    let max_cache_element: usize = if data_injection_config.n_members / (n_fragments * 20) != 0usize
    {
        data_injection_config.n_members / (n_fragments * 20)
    } else {
        1usize
    };
    let out_path = cli.output_path.unwrap_or(PathBuf::from("./generated"));

    let data_dump_path = cli.data_dump_path.unwrap_or(PathBuf::from(
        "../comunica_filter_benchmark/evaluation/data/dahcc_1_participant/data.ttl",
    ));

    let large_file = false;
    read_datadump(
        data_dump_path,
        &data_injection_config,
        notice_frequency,
        large_file,
        max_cache_element,
        n_fragments,
        out_path,
    )
    .unwrap();
    let duration = start.elapsed();

    println!("Time elapsed is {}", format_duration(duration));
    println!("--- Fragmentation finished---");
}
