use regex::Regex;
use serde;
use std::collections::HashSet;
use std::fs::read_to_string;
use std::path::PathBuf;

#[derive(serde::Deserialize, Debug, Clone)]
pub struct Config {
    pub member_url_regex: String,
    pub schema: Vec<Schema>,
    pub n_members: usize,
    pub date_field: String
}
impl Config {
    pub fn new(config_path: PathBuf) -> Self {
        let data = read_to_string(config_path).unwrap();
        let config: Config = serde_json::from_str(data.as_str()).unwrap();
        config
    }
}

#[derive(serde::Deserialize, Debug, Clone)]
pub struct Schema {
    pub subject: SubjectDescriptor,
    pub predicate: String,
    pub object: Option<SubjectDescriptor>,
}
impl Schema {
    pub fn is_valid(&self, input: &SchemaValidatorInput) -> bool {
        match &self.subject {
            SubjectDescriptor::MemberSubject => {
                let re = Regex::new(&input.member_url_regex).unwrap();
                if !re.is_match(&input.subject) {
                    return false;
                };
            }
            SubjectDescriptor::LinkedSubject { subject } => {
                if !input.related_subject.contains(subject) {
                    return false;
                }
            }
        };

        if self.predicate != input.predicate {
            return false;
        }

        return true;
    }
}
#[derive(serde::Deserialize, Debug, Clone)]
pub enum SubjectDescriptor {
    MemberSubject,
    LinkedSubject { subject: String },
}

pub struct SchemaValidatorInput {
    pub subject: String,
    pub predicate: String,
    pub object: String,
    pub member_url_regex: String,
    pub related_subject: HashSet<String>,
}
