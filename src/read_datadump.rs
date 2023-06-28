use super::config::*;
use super::member::*;
use chrono;
use regex;
use rio_api::parser::TriplesParser;
use rio_turtle;
use std::collections::HashSet;
use std::error::Error;
use std::fs::{read_to_string, File};
use std::io::BufReader;
use std::path::PathBuf;

pub fn read_datadump(
    path_data_dump: PathBuf,
    data_injection_config: &Config,
    notice_frequency: usize,
    large_file: bool,
) -> Result<(), Box<dyn Error>> {
    let file = File::open(path_data_dump.clone())?;
    let mut current_member = Member::default();
    let n_properties = data_injection_config.schema.len();
    let create_empy_valid_property = || -> Vec<bool> {
        let mut resp = Vec::with_capacity(n_properties);
        for _ in 0..n_properties {
            resp.push(false);
        }
        resp
    };
    let mut valid_properties = create_empy_valid_property();
    let full_property_valid = {
        let mut resp = Vec::with_capacity(n_properties);
        for _ in 0..n_properties {
            resp.push(true);
        }
        resp
    };
    let mut n_members_sorted = 0usize;
    let re_member_id = regex::Regex::new(&data_injection_config.member_url_regex).unwrap();

    let parsing_function = &mut |t: rio_api::model::Triple| -> Result<(), Box<dyn Error>> {
        // we give an id to the member we suppose that the first triple as has a subject the member IRI
        if current_member.properties.len() == 0 {
            current_member.id = t.subject.to_string();
        }

        // we add the triple as a property of the member
        current_member.properties.push(t.to_string());

        // we add the date into a specific field because it is the bases of the fragmentation
        if t.predicate.to_string() == data_injection_config.date_field {
            current_member.date = if let rio_api::model::Term::Literal(literal) = t.object {
                if let rio_api::model::Literal::Typed { value, datatype: _ } = literal {
                    chrono::NaiveDateTime::parse_from_str(
                        &value.to_string(),
                        "%Y-%m-%dT%H:%M:%S.%f",
                    )?.timestamp()
                } else {
                    panic!("the date object is not typed '{:?}'", t.to_string());
                }
            } else {
                panic!("the date object is not a literal '{:?}'", t.to_string());
            };
        }

        // we check the property of the member if they match the schema
        data_injection_config
            .schema
            .iter()
            .enumerate()
            .for_each(|(i, schema)| {
                let input = SchemaValidatorInput {
                    subject: t.subject.to_string(),
                    predicate: t.predicate.to_string(),
                    object: t.object.to_string(),
                    related_subject: HashSet::new(),
                };
                if schema.is_valid(&input, &re_member_id) {
                    valid_properties[i] = true;
                }
            });

        // the current member is materialized if it is complete
        if valid_properties == full_property_valid {
            //println!("{:?}", current_member);
            current_member = Member::new(n_properties);
            valid_properties = create_empy_valid_property();
            n_members_sorted += 1;
            if n_members_sorted % notice_frequency == 0 {
                println!(
                    "--- {:} out of {:} ({:?}%)---",
                    n_members_sorted,
                    data_injection_config.n_members,
                    (n_members_sorted as f32 / data_injection_config.n_members as f32) * 100f32
                );
            }
        }
        //println!("{:?}", valid_properties);
        Ok(())
    };
    if large_file {
        rio_turtle::TurtleParser::new(BufReader::new(file), None).parse_all(parsing_function)?;
    } else {
        rio_turtle::TurtleParser::new(read_to_string(path_data_dump)?.as_str().as_ref(), None)
            .parse_all(parsing_function)?;
    };

    Ok(())
}
