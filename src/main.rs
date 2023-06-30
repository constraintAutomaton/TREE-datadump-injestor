mod config;
mod fragment;
mod member;
mod read_datadump;

use chrono;
use config::*;
use fragment::*;
use futures;
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
    let out_path = PathBuf::from("./generated");
    let data_injection_config = Config::new(config_path);
    let notice_frequency = 1_000usize;
    let max_cache_element = 1_000;
    let n_fragments = 100;
    let max_cache_element = 1_000;
    let n_fragments = 10;
    let out_path = PathBuf::from("./generated");
    /** 
    let mut fragmentation = futures::executor::block_on(SimpleFragmentation::new(
        n_fragments,
        max_cache_element,
        &out_path,
        data_injection_config.highest_date.timestamp(),
        data_injection_config.lowest_date.timestamp(),
    ));
    */
    let path = PathBuf::from("/home/id357/Documents/PhD/coding/comunica_filter_benchmark/evaluation/data/dahcc_1_participant/data.ttl");
    let large_file = false;
    read_datadump(
        path,
        &data_injection_config,
        notice_frequency,
        large_file,
    )
    .unwrap();
    let duration = start.elapsed();

    println!("Time elapsed is {}", format_duration(duration));
    println!("--- Fragmentation finished---");
}
