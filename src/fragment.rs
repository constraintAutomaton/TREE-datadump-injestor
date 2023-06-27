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
    pub fn new(filename: &PathBuf) -> Self {
        fs::File::create(filename).unwrap();
        Self {
            filename: filename.clone(),
            boundary: Boundary::default(),
            size: 0usize,
            members_to_materialized: Vec::new(),
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
