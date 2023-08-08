use chrono;
use serde;
use std::collections::HashSet;
use std::fs::read_to_string;
use std::path::PathBuf;

/// Configuration of the parser, the member IRI must respect a regex expression
#[derive(serde::Deserialize, Debug, Clone)]
pub struct Config {
    /// A regex that the IRI of the members must respect
    pub member_url_regex: String,
    /// The [Schema]s of the member property
    pub schema: Vec<Schema>,
    /// The number of members, those not have any impact on the execution it is for being shown to the user
    pub n_members: usize,
    /// The date field for the fragmentation
    pub date_field: String,
    /// The highest date present in the data dump
    pub highest_date: chrono::NaiveDateTime,
    /// The lowest date present in the data dump
    pub lowest_date: chrono::NaiveDateTime,
    /// The address of the server that will host the TREE document
    pub server_address: String,
}
impl Config {
    pub fn new(config_path: PathBuf) -> Self {
        let data = read_to_string(config_path).unwrap();
        let config: Config = serde_json::from_str(data.as_str()).unwrap();
        config
    }
}

/// The schema of a triple pattern associated with a TREE member
#[derive(serde::Deserialize, Debug, Clone)]
pub struct Schema {
    pub subject: SubjectDescriptor,
    pub predicate: String,
    pub object: Option<SubjectDescriptor>,
}
impl Schema {
    /// check if a triple is valid with the schema
    pub fn is_valid(&self, input: &SchemaValidatorInput) -> bool {
        match &self.subject {
            SubjectDescriptor::MemberSubject => {
                if input.subject != input.member_id {
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

/// A descriptor for a subject in the context of a triple
#[derive(serde::Deserialize, Debug, Clone)]
pub enum SubjectDescriptor {
    /// The subject is the member
    MemberSubject,
    /// The subject is a property or subproperty of the member
    LinkedSubject { subject: String },
}

/// Input argument of the [Schema] [Schema::is_valid]
pub struct SchemaValidatorInput {
    pub subject: String,
    pub predicate: String,
    pub object: String,
    pub member_id: String,
    pub related_subject: HashSet<String>,
}
