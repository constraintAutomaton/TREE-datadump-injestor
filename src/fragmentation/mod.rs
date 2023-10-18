pub mod fragment;
pub mod linked_list_fragmentation;
pub mod one_ary_tree_fragmentation;
mod report;
pub mod tree;

use self::fragment::*;
use self::linked_list_fragmentation::LinkedListFragmentation;
use self::one_ary_tree_fragmentation::OneAryTreeFragmentation;
use self::report::Report;
use self::tree::Tree;
use crate::member::Member;
use crate::tree::*;
use serde_json;
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::PathBuf;

#[async_trait::async_trait]
pub trait Fragmentation {
    async fn insert(&mut self, member: &Member);
    async fn finalize(&mut self);
    fn max_size_cache(&self) -> usize;
    fn fragments(&self) -> &Vec<Fragment>;
    fn print_summary(&self) {
        for fragment in self.fragments().iter() {
            println!(
                "the boundaries of {fragment} are {} and it has {} members",
                fragment.boundary(),
                fragment.len()
            );
        }
        let n_members: usize = self
            .fragments()
            .iter()
            .map(|fragment| fragment.size())
            .sum();
        println!("there is {} members", n_members);
    }
}

pub(super) fn relations_to_string(relations: Vec<Relation>) -> String {
    let mut resp = String::new();
    for relation in relations {
        resp.push_str(&relation.to_string());
        resp.push_str("\n");
    }
    resp
}

pub(super) fn generate_central_root_node(
    folder: &PathBuf,
    n_fragments: usize,
    fragments: &Vec<Fragment>,
    fragmentation_property: &String,
    server_address: &String,
) {
    let filename = {
        let mut resp = folder.clone();
        resp.push(format!("0.ttl"));
        resp
    };

    let mut file = fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open(filename)
        .unwrap();
    let mut relations: Vec<Relation> = Vec::with_capacity(n_fragments);
    for fragment in fragments.iter() {
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
                fragmentation_property,
                server_address,
            ),
        );
    }
    let buffer = relations_to_string(relations);
    file.write_all(buffer.as_bytes()).unwrap();
}

pub(super) fn create_report(fragments: &Vec<Fragment>, folder: &PathBuf) {
    let mut map_report = HashMap::new();
    for fragment in fragments.iter() {
        let report = Report {
            n_member: if fragment.size() == 0 {
                None
            } else {
                Some(fragment.size())
            },
            boundary: fragment.boundary().clone(),
        };
        map_report.insert(fragment.filename().clone(), report);
        let json_string = serde_json::to_string(&map_report).expect("unable to produce the report");

        let report_path = {
            let mut resp = folder.clone();
            resp.push("report.json");
            resp
        };

        fs::write(report_path, json_string).expect("unable to generate the report file");
    }
}
#[derive(Clone, Debug)]
pub enum FragmentationTypeName {
    OneAryTree,
    LinkedList,
    Tree,
}

impl From<String> for FragmentationTypeName {
    fn from(item: String) -> Self {
        if item == "oneAryTree".to_string() {
            Self::OneAryTree
        } else if item == "linkedList".to_string() {
            Self::LinkedList
        } else if item == "tree".to_string() {
            Self::Tree
        } else {
            panic!("fragmentation {} not supported", item)
        }
    }
}

pub async fn factory(
    fragmentation_type: FragmentationTypeName,
    n_fragments_first_row: usize,
    max_size_cache: usize,
    folder: &PathBuf,
    highest_date: i64,
    lowest_date: i64,
    server_address: String,
    fragmentation_property: String,
    dept: Option<usize>,
    tree_id: String,
) -> Box<dyn Fragmentation> {
    match fragmentation_type {
        FragmentationTypeName::LinkedList => Box::new(
            LinkedListFragmentation::new(
                n_fragments_first_row,
                max_size_cache,
                &folder,
                highest_date,
                lowest_date,
                server_address,
                fragmentation_property,
                tree_id,
            )
            .await,
        ),
        FragmentationTypeName::OneAryTree => Box::new(
            OneAryTreeFragmentation::new(
                n_fragments_first_row,
                max_size_cache,
                &folder,
                highest_date,
                lowest_date,
                server_address,
                fragmentation_property,
                tree_id,
            )
            .await,
        ),
        FragmentationTypeName::Tree => Box::new(
            Tree::new(
                n_fragments_first_row,
                max_size_cache,
                &folder,
                highest_date,
                lowest_date,
                server_address,
                fragmentation_property,
                dept.expect("the dept should be defined to create a tree"),
                tree_id,
            )
            .await,
        ),
    }
}
