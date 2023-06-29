use super::member::Member;
use rand::Rng;
use std::fs;
use std::io::Write;
use std::ops::Deref;
use std::path::PathBuf;

pub struct Fragment {
    filename: PathBuf,
    pub boundary: Boundary,
    members_to_materialized: Vec<Member>,
    max_size_cache: usize,
}

impl Fragment {
    pub fn new(filename: &PathBuf, max_size_cache: usize) -> Self {
        fs::File::create(filename).unwrap();
        Self {
            filename: filename.clone(),
            boundary: Boundary::default(),
            members_to_materialized: Vec::with_capacity(max_size_cache),
            max_size_cache,
        }
    }

    pub fn materialize(&mut self) {
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

    pub fn insert(
        &mut self,
        member: &Member,
        relation_to_boundary: RelationToBoundary,
    ) -> Result<(), &str> {
        if self.max_size_cache >= self.members_to_materialized.len() + 1 {
            return Err("the member cache is full it has to be materialized");
        }
        self.members_to_materialized.push(member.clone());
        match relation_to_boundary {
            RelationToBoundary::Lower(_) => self.boundary.lower = member.date,
            RelationToBoundary::Greater(_) => self.boundary.upper = member.date,
            _ => {}
        };
        Ok(())
    }
}

pub struct Boundary {
    pub upper: i64,
    pub lower: i64,
}

impl Boundary {
    pub fn relation_with_boundery(&self, date: i64) -> RelationToBoundary {
        if date >= self.lower && date <= self.upper {
            RelationToBoundary::InBetween
        } else if date > self.upper {
            RelationToBoundary::Greater(self.upper - date)
        } else {
            RelationToBoundary::Lower(self.lower - date)
        }
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

#[derive(Clone, Copy)]
pub enum RelationToBoundary {
    InBetween,
    Lower(i64),
    Greater(i64),
}

impl Into<i64> for RelationToBoundary {
    fn into(self) -> i64 {
        match self {
            Self::InBetween => 0i64,
            Self::Greater(val) => val,
            Self::Lower(val) => val,
        }
    }
}

pub struct SimpleFragmentation {
    fragments: Vec<Fragment>,
    n_fragments: usize,
}

impl SimpleFragmentation {
    pub fn new(n_fragments: usize, max_size_cache: usize, folder: &PathBuf) -> Self {
        let fragments = {
            let mut resp: Vec<Fragment> = Vec::with_capacity(n_fragments);
            for i in 1..n_fragments {
                let fragment_path = {
                    let mut resp = folder.clone();
                    resp.push(i.to_string());
                    resp
                };
                resp.push(Fragment::new(&fragment_path, max_size_cache));
            }
            resp
        };
        Self {
            fragments,
            n_fragments,
        }
    }

    pub fn insert(&mut self, members: Vec<Member>) {
        for member in members.iter() {
            let mut scores: Vec<RelationToBoundary> = Vec::with_capacity(self.n_fragments);
            let mut max: i64 = i64::MIN;
            let mut pos = 0;
            let mut all_zero_score = true;
            for (i, fragment) in self.fragments.iter().enumerate() {
                let score = fragment.boundary.relation_with_boundery(member.date);
                let score_number: i64 = score.into();
                if score_number > max {
                    max = score_number;
                    pos = i;
                    if score_number != 0i64 {
                        all_zero_score = false;
                    }
                }
                scores.push(score);
            }
            if all_zero_score {
                pos = rand::thread_rng().gen_range(0..self.fragments.len());
            }

            if let Err(_) = self.fragments[pos].insert(member, scores[pos]) {
                self.fragments[pos].materialize();
                self.fragments[pos].insert(member, scores[pos]).unwrap();
            }
        }
    }
}
