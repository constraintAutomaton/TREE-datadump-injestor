use super::fragment::*;
use super::one_ary_tree_fragmentation::*;
use crate::member::Member;
use crate::tree::Relation;
use async_trait;
use futures;
use futures::stream::StreamExt;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use uuid;

pub struct LinkedListFragmentation {
    one_ary_tree_fragmentation: OneAryTreeFragmentation,
}

impl LinkedListFragmentation {
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
        let one_ary_tree_fragmentation = OneAryTreeFragmentation::new(
            n_fragments,
            max_size_cache,
            folder,
            highest_date,
            lowest_date,
            server_address,
            fragmentation_property,
            tree_id,
        )
        .await;

        Self {
            one_ary_tree_fragmentation,
        }
    }

    fn generate_root_node(&self) {
        let filename = {
            let mut resp = self.one_ary_tree_fragmentation.folder.clone();
            resp.push(format!("0.ttl"));
            resp
        };

        let mut file = fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(filename)
            .unwrap();
        let relation = Relation::new_unconstraint(
            format!("{}1.ttl", self.one_ary_tree_fragmentation.server_address),
            format!("{}0.ttl", self.one_ary_tree_fragmentation.server_address),
            uuid::Uuid::new_v4().to_string(),
        );

        let buffer = super::relations_to_string(vec![relation]);
        file.write_all(buffer.as_bytes()).unwrap();
    }
    fn set_up_boundary_to_infinity(&mut self) {
        for fragment in self.one_ary_tree_fragmentation.fragments.iter_mut() {
            fragment.up_boundary_infinity();
        }
    }
    async fn add_relation_to_nodes(&self) {
        let tasks = futures_util::stream::FuturesUnordered::new();
        for i in 0..self.one_ary_tree_fragmentation.n_fragments - 1 {
            let fragment_1 = &self.one_ary_tree_fragmentation.fragments[i];
            let fragment_2 = &self.one_ary_tree_fragmentation.fragments[i + 1];
            let relations = fragment_2.boundary().to_relation(
                &fragment_1
                    .filename()
                    .as_path()
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_string(),
                &fragment_2
                    .filename()
                    .as_path()
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_string(),
                &self.one_ary_tree_fragmentation.fragmentation_property,
                &self.one_ary_tree_fragmentation.server_address,
            );
            tasks.push(fragment_1.materialize_relation(relations));
        }
        let _: Vec<_> = tasks.collect().await;
    }
}

#[async_trait::async_trait]
impl super::Fragmentation for LinkedListFragmentation {
    async fn insert(&mut self, member: &Member) {
        self.one_ary_tree_fragmentation.insert(member).await;
    }

    async fn finalize(&mut self) {
        self.set_up_boundary_to_infinity();
        self.one_ary_tree_fragmentation.materialize().await;
        self.one_ary_tree_fragmentation.rebalance().await;
        self.generate_root_node();
        self.add_relation_to_nodes().await;
        self.print_summary();
        super::create_report(
            &self.one_ary_tree_fragmentation.fragments,
            &self.one_ary_tree_fragmentation.folder,
        );
    }

    fn max_size_cache(&self) -> usize {
        self.one_ary_tree_fragmentation.max_size_cache
    }

    fn fragments(&self) -> &Vec<Fragment> {
        &self.one_ary_tree_fragmentation.fragments
    }
}
