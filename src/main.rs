mod cli;
mod config;
mod fragmentation;
mod member;
mod parse_datadump;
mod tree;

use clap::Parser;
use cli::*;
use config::*;
use fragmentation::FragmentationTypeName;
use futures;
use futures::stream::StreamExt;
use glob;
use humantime::format_duration;
use parse_datadump::*;
use std::path::PathBuf;
use std::time;
use tokio;

#[tokio::main]
async fn main() {
    let start = time::Instant::now();
    let cli = Cli::parse();
    let config_path = cli.config_path.unwrap_or(PathBuf::from("./config.json"));
    let data_injection_config = Config::new(config_path);
    let notice_frequency = cli.frequency_notification;
    let n_fragments = cli.n_fragments;
    if n_fragments < 2 {
        panic!("The should be at least 2 fragments")
    }
    let dept = cli.dept;
    if let Some(dept) = dept {
        if dept <= 0 {
            panic!("the dept should be at least of 1")
        }
    }

    let max_cache_element: usize = if data_injection_config.n_members / (n_fragments * 20) != 0usize
    {
        data_injection_config.n_members / (n_fragments * 20)
    } else {
        1usize
    };
    let out_path = cli.output_path.unwrap_or(PathBuf::from("./generated"));
    delete_previous_file(&out_path).await;

    let data_dump_path = cli.data_dump_path.unwrap_or(PathBuf::from(
        "../comunica_filter_benchmark/evaluation/data/dahcc_1_participant/data.ttl",
    ));
    let large_file = cli.large_file;
    let fragmentation_type = if let Some(frag) = cli.fragmentation {
        FragmentationTypeName::from(frag)
    } else {
        FragmentationTypeName::OneAryTree
    };

    parse_datadump(
        data_dump_path,
        &data_injection_config,
        notice_frequency,
        large_file,
        max_cache_element,
        n_fragments,
        out_path,
        fragmentation_type,
        dept
    )
    .unwrap();
    let duration = start.elapsed();

    println!("Time elapsed is {}", format_duration(duration));
    println!("--- Fragmentation finished---");
}

async fn delete_previous_file(out_path: &PathBuf) {
    let mut tasks = Vec::new();
    println!("{}/*.ttl", out_path.as_path().to_str().unwrap());
    for path in glob::glob(&format!("{}/*.ttl", out_path.as_path().to_str().unwrap())).unwrap() {
        tasks.push(async {
            match path {
                Ok(path) => {
                    println!("Removing file: {:?}", path.display());
                    std::fs::remove_file(path).unwrap();
                }
                Err(e) => panic!("{e}"),
            }
        });
    }
    let task_stream: futures_util::stream::FuturesUnordered<_> = tasks.into_iter().collect();

    let _: Vec<_> = task_stream.collect().await;
}
