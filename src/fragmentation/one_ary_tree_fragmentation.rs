use crate::member::Member;
use crate::tree::*;
use super::fragment::*;
use async_trait;
use chrono;
use futures;
use futures::stream::StreamExt;
use std::fs;
use std::io::Write;
use std::path::PathBuf;

pub struct OneAryTreeFragmentation {
    pub(super) fragments: Vec<Fragment>,
    pub(super) n_fragments: usize,
    pub(super) max_size_cache: usize,
    pub(super) folder: PathBuf,
    pub(super) server_address: String,
    pub(super) fragmentation_property: String,
}

impl OneAryTreeFragmentation {
    pub async fn new(
        n_fragments: usize,
        max_size_cache: usize,
        folder: &PathBuf,
        highest_date: i64,
        lowest_date: i64,
        server_address: String,
        fragmentation_property: String,
    ) -> Self {
        let fragments = {
            let tasks = futures_util::stream::FuturesUnordered::new();
            let mut current_lower_bound = lowest_date;

            let increment = ((highest_date as f32 - lowest_date as f32) / n_fragments as f32).ceil() as i64;
            for i in 0..n_fragments {
                let fragment_path = {
                    let mut resp = folder.clone();
                    resp.push(format!("{}.ttl", i + 1));
                    resp
                };

                tasks.push(Fragment::new(
                    fragment_path,
                    max_size_cache,
                    if i == 0 {
                        chrono::NaiveDateTime::MIN.timestamp()
                    } else {
                        current_lower_bound
                    },
                    if i == n_fragments - 1 {
                        chrono::NaiveDateTime::MAX.timestamp()
                    } else {
                        current_lower_bound + increment
                    },
                ));
                current_lower_bound += increment;
            }
            let resp: Vec<Fragment> = tasks.collect().await;
            resp
        };
        for (i, fragment) in fragments.iter().enumerate() {
            println!("the boundaries of {i} are {}", fragment.boundary(),);
        }
        Self {
            fragments,
            n_fragments,
            max_size_cache,
            folder: folder.clone(),
            server_address,
            fragmentation_property,
        }
    }

    pub async fn materialize(&mut self) {
        let mut materialize_tasks = Vec::with_capacity(self.n_fragments);
        for fragment in self.fragments.iter_mut() {
            materialize_tasks.push(fragment.materialize());
        }
        let task_stream: futures_util::stream::FuturesUnordered<_> =
            materialize_tasks.into_iter().collect();
        task_stream.collect().await
    }

    fn generate_root_node(&self) {
        let filename = {
            let mut resp = self.folder.clone();
            resp.push(format!("0.ttl"));
            resp
        };

        let mut file = fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(filename)
            .unwrap();
        let mut relations: Vec<Relation> = Vec::with_capacity(self.n_fragments);
        for fragment in self.fragments.iter() {
            relations.append(
                &mut fragment.boundary().to_relation(
                    &"0.ttl".to_string(),
                    &fragment
                        .filename()
                        .as_path()
                        .file_name()
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .to_string(),
                    &self.fragmentation_property,
                    &self.server_address,
                ),
            );
        }
        let buffer = Self::relations_to_string(relations);
        file.write_all(buffer.as_bytes()).unwrap();
    }

    pub (super) fn relations_to_string(relations: Vec<Relation>) -> String {
        let mut resp = String::new();
        for relation in relations {
            resp.push_str(&relation.to_string());
            resp.push_str("\n");
        }
        resp
    }

    /// It simply delete the fragment with a size of 0, and merge two adjacent fragment
    /// if the current fragment has 10 times less members than the average.
    pub (super) async fn rebalance(&mut self) {
        self.fragments.retain(|fragment| {
            if fragment.size() == 0 {
                fragment.clear_file();
                false
            } else {
                true
            }
        });
        self.n_fragments = self.fragments.len();
    }
}

#[async_trait::async_trait]
impl super::Fragmentation for OneAryTreeFragmentation {
    async fn insert(&mut self, member: &Member) {
        let mut pos = 0;
        for (i, fragment) in self.fragments.iter().enumerate() {
            if fragment.boundary().is_in_between(member.date) {
                pos = i;
                break;
            }
        }
        if let Err(_) = self.fragments[pos].insert(&member) {
            self.materialize().await;
            self.fragments[pos].insert(&member).unwrap();
        }
    }
    async fn finalize(&mut self) {
        self.materialize().await;
        self.rebalance().await;
        self.generate_root_node();
        self.print_summary();
    }

    fn fragments(&self) -> &Vec<Fragment> {
        &self.fragments
    }

    fn max_size_cache(&self) -> usize {
        self.max_size_cache
    }
}