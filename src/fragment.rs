use super::member::Member;
use super::tree::*;
use async_trait;
use chrono;
use futures;
use futures::stream::StreamExt;
use std::fmt;
use std::fs;
use std::io::Write;
use std::ops::Deref;
use std::path::PathBuf;
use uuid;

#[derive(Clone)]
pub struct Fragment {
    filename: PathBuf,
    boundary: Boundary,
    members_to_materialized: Vec<Member>,
    max_size_cache: usize,
    size: usize,
}

impl Fragment {
    pub async fn new(
        filename: PathBuf,
        max_size_cache: usize,
        lower_bound: i64,
        upper_bound: i64,
    ) -> Self {
        fs::File::create(&filename).unwrap();
        Self {
            filename: filename.clone(),
            boundary: Boundary {
                lower: lower_bound,
                upper: upper_bound,
            },
            members_to_materialized: Vec::with_capacity(max_size_cache),
            max_size_cache,
            size: 0,
        }
    }

    pub fn boundary(&self) -> &Boundary {
        &self.boundary
    }

    pub fn filename(&self) -> &PathBuf {
        &self.filename
    }

    pub async fn materialize_relation(&self, relations: Vec<Relation>) {
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&self.filename)
            .unwrap();
        for relation in relations {
            let buffer = relation.to_string();
            file.write_all(buffer.as_bytes()).unwrap();
        }
    }
    pub async fn materialize(&mut self) {
        if self.members_to_materialized.len() > 0 {
            let mut file = fs::OpenOptions::new()
                .append(true)
                .open(&self.filename)
                .unwrap();
            let buffer = {
                let mut resp = String::new();
                for member in self.members_to_materialized.iter() {
                    resp.push_str(&member.to_string())
                }
                resp
            };
            file.write_all(buffer.as_bytes()).unwrap();
            self.members_to_materialized = Vec::new();
        }
    }

    pub fn clear_file(&self) {
        fs::remove_file(&self.filename).expect("was not able to delete the fragment");
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn insert(&mut self, member: &Member) -> Result<(), &str> {
        if self.members_to_materialized.len() + 1 >= self.max_size_cache {
            return Err("the member cache is full it has to be materialized");
        }
        self.size += 1;
        self.members_to_materialized.push(member.clone());
        Ok(())
    }
}

impl fmt::Display for Fragment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.filename.as_os_str().to_str().unwrap())
    }
}
#[derive(Debug, Clone)]
pub struct Boundary {
    pub upper: i64,
    pub lower: i64,
}

impl fmt::Display for Boundary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let upper = chrono::NaiveDateTime::from_timestamp_opt(self.upper, 0)
            .unwrap_or(chrono::NaiveDateTime::MAX);
        let lower = chrono::NaiveDateTime::from_timestamp_opt(self.lower, 0)
            .unwrap_or(chrono::NaiveDateTime::MIN);
        write!(f, "[{upper}, {lower}]")
    }
}
impl Boundary {
    pub fn is_in_between(&self, date: i64) -> bool {
        date >= self.lower && date <= self.upper
    }

    pub fn to_relation(
        &self,
        current_id: &String,
        destination_id: &String,
        fragmentation_property: &String,
        server_address: &String,
    ) -> Vec<Relation> {
        let mut resp: Vec<Relation> = Vec::new();
        if self.upper < chrono::NaiveDateTime::MAX.timestamp() {
            resp.push(Self::create_relation(
                self.upper,
                fragmentation_property,
                server_address,
                destination_id,
                current_id,
                RelationOperator::LessThanRelation,
            ));
        }

        if self.lower > chrono::NaiveDateTime::MIN.timestamp() {
            resp.push(Self::create_relation(
                self.lower,
                fragmentation_property,
                server_address,
                destination_id,
                current_id,
                RelationOperator::GreaterThanOrEqualToRelation,
            ));
        }

        resp
    }

    fn create_relation(
        time_value: i64,
        fragmentation_property: &String,
        server_address: &String,
        destination_id: &String,
        current_id: &String,
        relation_type: RelationOperator,
    ) -> Relation {
        Relation::new(
            fragmentation_property.clone(),
            chrono::NaiveDateTime::from_timestamp_opt(time_value, 0)
                .unwrap()
                .format(DATE_TIME_FORMAT)
                .to_string(),
            format!("{server_address}{destination_id}"),
            relation_type,
            format!("{server_address}{current_id}"),
            uuid::Uuid::new_v4().to_string(),
        )
    }
}

impl Default for Boundary {
    fn default() -> Self {
        Self {
            upper: i64::MAX,
            lower: i64::MIN,
        }
    }
}

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
                fragment.boundary,
                fragment.len()
            );
        }
    }
}

pub struct OneAryTreeFragmentation {
    fragments: Vec<Fragment>,
    n_fragments: usize,
    max_size_cache: usize,
    folder: PathBuf,
    server_address: String,
    fragmentation_property: String,
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

            let increment = (highest_date - lowest_date) / n_fragments as i64;
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
                &mut fragment.boundary.to_relation(
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

    fn relations_to_string(relations: Vec<Relation>) -> String {
        let mut resp = String::new();
        for relation in relations {
            resp.push_str(&relation.to_string());
            resp.push_str("\n");
        }
        resp
    }

    /// It simply delete the fragment with a size of 0, and merge two adjacent fragment
    /// if the current fragment has 10 times less members than the average.
    async fn rebalance(&mut self) {
        self.fragments.retain(|fragment| {
            if fragment.size == 0 {
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
impl Fragmentation for OneAryTreeFragmentation {
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
    ) -> Self {
        let one_ary_tree_fragmentation = OneAryTreeFragmentation::new(
            n_fragments,
            max_size_cache,
            folder,
            highest_date,
            lowest_date,
            server_address,
            fragmentation_property,
        )
        .await;

        Self {
            one_ary_tree_fragmentation,
        }
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
        let relation = self.fragments[0].boundary.to_relation(
            &"0.ttl".to_string(),
            &"1.ttl".to_string(),
            &self.fragmentation_property,
            &self.server_address,
        );
        let buffer = OneAryTreeFragmentation::relations_to_string(relation);
        file.write_all(buffer.as_bytes()).unwrap();
    }

    async fn add_relation_to_nodes(&self) {
        let tasks = futures_util::stream::FuturesUnordered::new();
        for i in 0..self.n_fragments - 1 {
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
                &self.fragmentation_property,
                &self.server_address,
            );
            tasks.push(fragment_1.materialize_relation(relations));
        }
        let _: Vec<_> = tasks.collect().await;
    }
}

#[async_trait::async_trait]
impl Fragmentation for LinkedListFragmentation {
    async fn insert(&mut self, member: &Member) {
        self.one_ary_tree_fragmentation.insert(member).await;
    }

    async fn finalize(&mut self) {
        self.one_ary_tree_fragmentation.materialize().await;
        self.one_ary_tree_fragmentation.rebalance().await;
        self.generate_root_node();
        self.add_relation_to_nodes().await;
        self.print_summary();
    }

    fn max_size_cache(&self) -> usize {
        self.one_ary_tree_fragmentation.max_size_cache
    }

    fn fragments(&self) -> &Vec<Fragment> {
        &self.fragments
    }
}

impl Deref for LinkedListFragmentation {
    type Target = OneAryTreeFragmentation;

    fn deref(&self) -> &Self::Target {
        &self.one_ary_tree_fragmentation
    }
}
const DATE_TIME_FORMAT: &'static str = "%Y-%m-%dT%H:%M:%S.%f";
