mod config;
mod domain;
mod read_datadump;
mod fragment;

use config::*;
use read_datadump::*;
use std::{error::Error, path::PathBuf};

fn main() -> Result<(), Box<dyn Error>> {
    let config_path = PathBuf::from("./config_dahcc.json");
    let data_injection_config = Config::new(config_path);
    let notice_frequency = 1000usize;
    let path = PathBuf::from("/home/id357/Documents/PhD/coding/comunica_filter_benchmark/evaluation/data/dahcc_1_participant/data.ttl");
    let large_file = false;
    read_datadump(path, &data_injection_config, notice_frequency, large_file)?;

    println!("--- Fragmentation finished---");
    Ok(())
}
