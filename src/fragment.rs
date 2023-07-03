use super::member::Member;
use super::tree::*;
use async_trait;
use chrono;
use futures;
use futures::stream::StreamExt;
use std::cell::RefCell;
use std::fmt;
use std::fs;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::rc::Rc;
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

    pub fn absorb_lower_fragment(&mut self, fragment: &mut Fragment) -> Result<(), &'static str> {
        if fragment.boundary.lower < self.boundary.lower {
            let mut file = fs::OpenOptions::new()
                .append(true)
                .open(&self.filename)
                .unwrap();
            if fragment.size < BIG_FILE_N_MEMBER {
                let content = fs::read_to_string(&fragment.filename).unwrap();
                file.write_all(content.as_bytes()).unwrap();
            } else {
                let fragment_file = fs::OpenOptions::new()
                    .read(true)
                    .open(&fragment.filename)
                    .unwrap();

                let chunk_size = 1_000_000_000;

                loop {
                    let mut chunk = Vec::with_capacity(chunk_size);
                    let n = (&fragment_file)
                        .take(chunk_size as u64)
                        .read_to_end(&mut chunk)
                        .unwrap();
                    if n == 0 {
                        break;
                    } else {
                        file.write_all(&chunk).unwrap();
                    }
                }
            }
            self.boundary.lower = fragment.boundary.lower;
            self.size += fragment.size;
            fragment.clear();
            Ok(())
        } else {
            Err("the fragment don't contain data comming from before the current fragment")
        }
    }

    pub fn clear(&mut self) {
        self.members_to_materialized.clear();
        self.size = 0;
        self.boundary = Boundary::default();
        fs::remove_file(&self.filename).expect("was not able to delete the fragment");
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
}

pub struct SimpleFragmentation {
    fragments: Vec<Fragment>,
    n_fragments: usize,
    max_size_cache: usize,
    folder: PathBuf,
    server_address: String,
    fragmentation_property: String,
}

impl SimpleFragmentation {
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
            let mut tasks = Vec::with_capacity(n_fragments);
            let mut current_lower_bound = lowest_date;

            let increment = ((highest_date - lowest_date) as f64 / n_fragments as f64) as i64;
            for i in 0..n_fragments {
                let fragment_path = {
                    let mut resp = folder.clone();
                    resp.push(format!("{}.ttl", i + 1));
                    resp
                };

                tasks.push(Fragment::new(
                    fragment_path,
                    max_size_cache,
                    if current_lower_bound == lowest_date {
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
            let task_stream: futures_util::stream::FuturesUnordered<_> =
                tasks.into_iter().collect();
            let resp: Vec<Fragment> = task_stream.collect().await;
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
        let mut materilize_tasks = Vec::with_capacity(self.n_fragments);
        for fragment in self.fragments.iter_mut() {
            materilize_tasks.push(fragment.materialize());
        }
        let task_stream: futures_util::stream::FuturesUnordered<_> =
            materilize_tasks.into_iter().collect();
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
        let mut relations: Vec<Relation> = Vec::new();
        for (i, fragment) in self.fragments.iter().enumerate() {
            relations.append(&mut self.create_relation(
                fragment.boundary(),
                format!("{}.ttl", i + 1),
                uuid::Uuid::new_v4().to_string(),
                uuid::Uuid::new_v4().to_string(),
            ));
        }
        let buffer = Self::relations_to_string(relations);
        file.write_all(buffer.as_bytes()).unwrap();
    }

    fn create_relation(
        &self,
        boundary: &Boundary,
        fragment_id: String,
        relation_id_1: String,
        relation_id_2: String,
    ) -> Vec<Relation> {
        let mut resp: Vec<Relation> = Vec::new();
        let date_time_format = "%Y-%m-%dT%H:%M:%S.%f";
        if boundary.upper < chrono::NaiveDateTime::MAX.timestamp() {
            resp.push(Relation::new(
                self.fragmentation_property.clone(),
                chrono::NaiveDateTime::from_timestamp_opt(boundary.upper, 0)
                    .unwrap()
                    .format(date_time_format)
                    .to_string(),
                format!("{}{}", self.server_address, fragment_id),
                RelationOperator::LessThanRelation,
                format!("{}0.ttl", self.server_address),
                relation_id_1,
            ));
        }

        if boundary.lower > chrono::NaiveDateTime::MIN.timestamp() {
            resp.push(Relation::new(
                self.fragmentation_property.clone(),
                chrono::NaiveDateTime::from_timestamp_opt(boundary.lower, 0)
                    .unwrap()
                    .format(date_time_format)
                    .to_string(),
                format!("{}{}", self.server_address, fragment_id),
                RelationOperator::GreaterThanOrEqualToRelation,
                format!("{}0.ttl", self.server_address),
                relation_id_2,
            ));
        }

        resp
    }

    fn relations_to_string(relations: Vec<Relation>) -> String {
        let mut resp = String::new();
        for relation in relations {
            resp.push_str(&relation.to_string());
            resp.push_str("\n");
        }
        resp
    }

    fn average_n_members(&self) -> f32 {
        let sum: f32 = self
            .fragments
            .iter()
            .map(move |fragment| fragment.size as f32)
            .sum();

        sum / self.n_fragments as f32
    }

    async fn rebalance(&mut self) {
        let avg_n_members = self.average_n_members();

        let shared_fragments = Rc::new(RefCell::new(self.fragments.clone()));
        for i in 0..self.n_fragments - 2 {
            if self.fragments[i].size <= 2 * avg_n_members as usize {
                let current_fragment = &mut shared_fragments.borrow_mut()[i];
                let next_fragment = &mut shared_fragments.borrow_mut()[i + 1];
                next_fragment.absorb_lower_fragment(current_fragment).unwrap();
            }
        }
        self.fragments = shared_fragments.take();
        self.fragments.retain_mut(|fragment| {
            if fragment.size == 0 {
                fragment.clear();
                false
            } else {
                true
            }
        });
    }
}

#[async_trait::async_trait]
impl Fragmentation for SimpleFragmentation {
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

        for (i, fragment) in self.fragments.iter().enumerate() {
            println!(
                "the boundaries of {i} are {} and it has {} members",
                fragment.boundary,
                fragment.len()
            );
        }
    }
    fn max_size_cache(&self) -> usize {
        self.max_size_cache
    }
}

const BIG_FILE_N_MEMBER: usize = 5_000_000;
