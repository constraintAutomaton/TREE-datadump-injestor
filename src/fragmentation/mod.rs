pub mod fragment;
pub mod linked_list_fragmentation;
pub mod one_ary_tree_fragmentation;

use self::fragment::*;
use self::linked_list_fragmentation::LinkedListFragmentation;
use self::one_ary_tree_fragmentation::OneAryTreeFragmentation;
use crate::member::Member;
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
    }
}

#[derive(Clone, Debug)]
pub enum FragmentationTypeName {
    OneAryTree,
    LinkedList,
}

impl From<String> for FragmentationTypeName {
    fn from(item: String) -> Self {
        if item == "oneAryTree".to_string() {
            Self::OneAryTree
        } else if item == "linkedList".to_string() {
            Self::LinkedList
        } else {
            panic!("fragmentation {} not supported", item)
        }
    }
}

pub async fn factory(
    fragmentation_type: FragmentationTypeName,
    n_fragments: usize,
    max_size_cache: usize,
    folder: &PathBuf,
    highest_date: i64,
    lowest_date: i64,
    server_address: String,
    fragmentation_property: String,
) -> Box<dyn Fragmentation> {
    match fragmentation_type {
        FragmentationTypeName::LinkedList => Box::new(
            LinkedListFragmentation::new(
                n_fragments,
                max_size_cache,
                &folder,
                highest_date,
                lowest_date,
                server_address,
                fragmentation_property,
            )
            .await,
        ),
        FragmentationTypeName::OneAryTree => Box::new(
            OneAryTreeFragmentation::new(
                n_fragments,
                max_size_cache,
                &folder,
                highest_date,
                lowest_date,
                server_address,
                fragmentation_property,
            )
            .await,
        ),
    }
}
