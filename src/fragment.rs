use super::member::Member;
use async_trait;
use chrono;
use std::fmt;
use std::fs;
use std::io::Write;
use std::path::PathBuf;

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

    pub async fn materialize(&mut self) {
        if self.members_to_materialized.len() > 0 {
            let mut file = fs::OpenOptions::new()
                .append(true)
                .open(&self.filename.clone())
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

#[derive(Debug)]
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
}

impl SimpleFragmentation {
    pub async fn new(
        n_fragments: usize,
        max_size_cache: usize,
        folder: &PathBuf,
        highest_date: i64,
        lowest_date: i64,
    ) -> Self {
        let fragments = {
            let mut resp = Vec::with_capacity(n_fragments);
            let mut current_lower_bound = lowest_date;

            let increment = (highest_date - lowest_date) / (n_fragments - 1) as i64;
            for i in 1..n_fragments {
                let fragment_path = {
                    let mut resp = folder.clone();
                    resp.push(format!("{}.ttl", i));
                    resp
                };

                resp.push(Fragment::new(
                    fragment_path,
                    max_size_cache,
                    if current_lower_bound == lowest_date {
                        chrono::NaiveDateTime::MIN.timestamp()
                    } else {
                        current_lower_bound
                    },
                    if current_lower_bound + increment >= highest_date {
                        chrono::NaiveDateTime::MAX.timestamp()
                    } else {
                        current_lower_bound + increment
                    },
                ));
                current_lower_bound += increment + 1;
            }
            futures::future::join_all(resp).await
        };
        for (i, fragment) in fragments.iter().enumerate() {
            println!("the boundaries of {i} are {}", fragment.boundary(),);
        }
        Self {
            fragments,
            n_fragments,
            max_size_cache,
        }
    }

    pub async fn materialize(&mut self) {
        let mut materilize_tasks = Vec::with_capacity(self.n_fragments);
        for fragment in self.fragments.iter_mut() {
            materilize_tasks.push(fragment.materialize());
        }
        futures::future::join_all(materilize_tasks).await;
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
