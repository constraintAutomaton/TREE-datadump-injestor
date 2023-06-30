mod config;
mod fragment;
mod member;
mod read_datadump;
mod tree;

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
    let config_path = PathBuf::from("./config.json");
    let data_injection_config = Config::new(config_path);
    let notice_frequency = 1_000usize;
    let max_cache_element = 100;
    let n_fragments = 1000;
    let out_path = PathBuf::from("./generated");

    let path = PathBuf::from("/home/id357/Documents/PhD/coding/comunica_filter_benchmark/evaluation/data/dahcc_1_participant/data.ttl");
    let large_file = false;
    read_datadump(
        path,
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
