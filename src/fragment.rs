use super::member::Member;
use chrono;
use std::fs;
use std::io::Write;
use std::path::PathBuf;

pub struct Fragment {
    filename: PathBuf,
    pub boundary: Boundary,
    size: usize,
    members_to_materialized: Vec<Member>,
}

impl Fragment {
    pub fn new(filename: &PathBuf, max_size_cache: usize) -> Self {
        fs::File::create(filename).unwrap();
        Self {
            filename: filename.clone(),
            boundary: Boundary::default(),
            size: 0usize,
            members_to_materialized: Vec::with_capacity(max_size_cache),
        }
    }

    pub async fn materialize(&self) {
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
    }

    pub fn len(&self) -> usize {
        self.size
    }
}

pub struct Boundary {
    pub up: chrono::NaiveDate,
    pub down: chrono::NaiveDate,
}

impl Default for Boundary {
    fn default() -> Self {
        Self {
            up: chrono::NaiveDate::MAX,
            down: chrono::NaiveDate::MIN,
        }
    }
}

pub struct SimpleFragmentation {
    fragments: Vec<Fragment>,
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
                resp.push(Fragment::new(&fragment_path, max_size_cache))
            }
            resp
        };
        Self { fragments }
    }
}
