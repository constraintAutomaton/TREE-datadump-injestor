use super::Fragment;
use crate::member::Member;
use futures;
use futures::stream::StreamExt;
use rand::{self, Rng, SeedableRng};
use std::path::PathBuf;

pub struct Tree {
    fragments: Vec<Fragment>,
    max_size_cache: usize,
    random_generator: rand::rngs::StdRng,
    folder: PathBuf,
}

impl Tree {
    pub async fn new(
        n_fragments: usize,
        max_size_cache: usize,
        folder: &PathBuf,
        highest_date: i64,
        lowest_date: i64,
        server_address: String,
        fragmentation_property: String,
        dept: usize,
    ) -> Self {
        let fragments = {
            let tasks_create_first_row = futures_util::stream::FuturesUnordered::new();
            let mut current_lower_bound = lowest_date;

            let increment =
                ((highest_date as f32 - lowest_date as f32) / n_fragments as f32).ceil() as i64;
            for i in 0..n_fragments {
                let fragment_path = {
                    let mut resp = folder.clone();
                    resp.push(format!("{}.ttl", i + 1));
                    resp
                };

                tasks_create_first_row.push(Fragment::new(
                    fragment_path,
                    max_size_cache,
                    if i == 0 {
                        current_lower_bound - increment
                    } else {
                        current_lower_bound
                    },
                    if i == n_fragments - 1 {
                        current_lower_bound + 2 * increment
                    } else {
                        current_lower_bound + increment
                    },
                ));
                current_lower_bound += increment;
            }
            let mut resp: Vec<Fragment> = tasks_create_first_row.collect().await;
            super::generate_central_root_node(
                folder,
                n_fragments,
                &resp,
                &fragmentation_property,
                &server_address,
            );
            let mut fragment_to_divide = resp.clone();

            for i in 0..dept {
                let mut current_fragment = fragment_to_divide.pop();
                let mut next_fragments_to_divide = Vec::with_capacity(n_fragments * (i + 1));
                while let Some(fragment) = current_fragment.as_mut() {
                    let (fragment_1, fragment_2) = fragment
                        .create_two_sub_fragment(&fragmentation_property, &server_address)
                        .await;
                    next_fragments_to_divide.push(fragment_1.clone());
                    next_fragments_to_divide.push(fragment_2.clone());
                    resp.push(fragment_1.clone());
                    resp.push(fragment_2.clone());
                    current_fragment = fragment_to_divide.pop();
                }
                fragment_to_divide.append(&mut next_fragments_to_divide);
            }
            resp
        };
        super::create_report(&fragments, &folder);
        Self {
            fragments,
            max_size_cache,
            random_generator: rand::rngs::StdRng::from_entropy(),
            folder: folder.clone(),
        }
    }

    async fn materialize(&mut self) {
        let materialize_tasks = futures_util::stream::FuturesUnordered::new();
        for fragment in self.fragments.iter_mut() {
            materialize_tasks.push(fragment.materialize());
        }

        let _: Vec<_> = materialize_tasks.collect().await;
    }
}

#[async_trait::async_trait]
impl super::Fragmentation for Tree {
    async fn insert(&mut self, member: &Member) {
        let mut pos_candidate = Vec::new();
        for (i, fragment) in self.fragments.iter().enumerate() {
            if fragment.boundary().is_in_between(member.date) {
                pos_candidate.push(i);
            }
        }
        let pos = pos_candidate[self.random_generator.gen_range(0..pos_candidate.len())];
        if let Err(_) = self.fragments[pos].insert(&member) {
            self.materialize().await;
            self.fragments[pos].insert(&member).unwrap();
        }
    }
    async fn finalize(&mut self) {
        self.materialize().await;
        self.print_summary();
        super::create_report(&self.fragments, &self.folder);
    }
    fn max_size_cache(&self) -> usize {
        self.max_size_cache
    }
    fn fragments(&self) -> &Vec<Fragment> {
        &self.fragments
    }
}
