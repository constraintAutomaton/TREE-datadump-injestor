use crate::member::Member;
use crate::tree::*;
use chrono;
use std::fmt;
use std::fs;
use std::io::Write;
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

    pub fn size(&self) -> usize {
        self.size
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

    pub async fn create_two_sub_fragment(
        &mut self,
        fragmentation_property: &String,
        server_address: &String,
    ) -> (Fragment, Fragment) {
        self.materialize().await;

        let mid_bound = self.boundary.lower + (self.boundary.upper - self.boundary.lower) / 2;
        let generate_filename = || {
            let mut resp = self.filename.clone();
            resp.pop();
            resp.push(format!("{}.ttl", uuid::Uuid::new_v4().to_string()));
            resp
        };

        let fragment_1 = Fragment::new(
            generate_filename(),
            self.max_size_cache,
            self.boundary.lower,
            mid_bound,
        )
        .await;

        let fragment_2 = Fragment::new(
            generate_filename(),
            self.max_size_cache,
            mid_bound,
            self.boundary.upper,
        )
        .await;

        let relations_1 = fragment_1.boundary.to_relation(
            &self
                .filename()
                .as_path()
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .to_string(),
            &fragment_1
                .filename()
                .as_path()
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .to_string(),
            fragmentation_property,
            server_address,
        );

        let relations_2 = fragment_2.boundary.to_relation(
            &self
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
            fragmentation_property,
            server_address,
        );

        self.materialize_relation(relations_1).await;
        self.materialize_relation(relations_2).await;

        (fragment_1, fragment_2)
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

const DATE_TIME_FORMAT: &'static str = "%Y-%m-%dT%H:%M:%S.%f";
