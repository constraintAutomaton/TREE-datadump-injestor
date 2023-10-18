use super::fragment::*;
use crate::member::Member;
use async_trait;
use chrono;
use futures;
use futures::stream::StreamExt;
use std::path::PathBuf;

pub struct OneAryTreeFragmentation {
    pub(super) fragments: Vec<Fragment>,
    pub(super) n_fragments: usize,
    pub(super) max_size_cache: usize,
    pub(super) folder: PathBuf,
    pub(super) server_address: String,
    pub(super) fragmentation_property: String,
    tree_id: String,
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
        tree_id: String,
    ) -> Self {
        let fragments = {
            let tasks = futures_util::stream::FuturesUnordered::new();
            let mut current_lower_bound = lowest_date;

            let increment =
                ((highest_date as f32 - lowest_date as f32) / n_fragments as f32).ceil() as i64;
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

        super::generate_central_root_node(
            &folder,
            n_fragments,
            &fragments,
            &fragmentation_property,
            &server_address,
        );

        super::create_report(&fragments, &folder);

        Self {
            fragments,
            n_fragments,
            max_size_cache,
            folder: folder.clone(),
            server_address,
            fragmentation_property,
            tree_id,
        }
    }

    /// It simply delete the fragment with a size of 0, and merge two adjacent fragment
    /// if the current fragment has 10 times less members than the average.
    pub(super) async fn rebalance(&mut self) {
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

    pub(super) async fn materialize(&mut self) {
        let materialize_tasks = futures_util::stream::FuturesUnordered::new();
        for fragment in self.fragments.iter_mut() {
            materialize_tasks.push(fragment.materialize(&self.tree_id));
        }

        let _: Vec<_> = materialize_tasks.collect().await;
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
        self.print_summary();
        super::create_report(&self.fragments, &self.folder);
    }

    fn fragments(&self) -> &Vec<Fragment> {
        &self.fragments
    }

    fn max_size_cache(&self) -> usize {
        self.max_size_cache
    }
}
