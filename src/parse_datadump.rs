use super::config::*;
use super::fragment::*;
use super::member::*;
use chrono;
use regex;
use rio_api::parser::TriplesParser;
use rio_turtle;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::error::Error;
use std::fs::{read_to_string, File};
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;
use tokio;

pub fn parse_datadump(
    data_dump_path: PathBuf,
    data_injection_config: &Config,
    notice_frequency: usize,
    large_file: bool,
    max_cache_element: usize,
    n_fragments: usize,
    out_path: PathBuf,
) -> Result<(), Box<dyn Error>> {
    let file = File::open(data_dump_path.clone())?;
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

    let mut n_member_parsed = 0usize;
    let re_member_id = regex::Regex::new(&data_injection_config.member_url_regex).unwrap();

    let (tx_member, rx_member) = mpsc::channel();

    let handle = tokio::runtime::Handle::current();
    let parsing_function = &mut |t: rio_api::model::Triple| -> Result<(), Box<dyn Error>> {
        // we give an id to the member we suppose that the first triple as has a subject the member IRI
        if current_member.properties.len() == 0 {
            let id = t.subject.to_string();
            if re_member_id.is_match(&id) {
                current_member.id = id;
            }
        }

        // we add the triple as a property of the member
        current_member.properties.push(t.to_string());

        // we add the date into a specific field because it is the bases of the fragmentation
        if t.predicate
            .to_string()
            .contains(&data_injection_config.date_field)
        {
            current_member.date = if let rio_api::model::Term::Literal(literal) = t.object {
                if let rio_api::model::Literal::Typed { value, datatype: _ } = literal {
                    chrono::NaiveDateTime::parse_from_str(
                        &value.to_string(),
                        "%Y-%m-%dT%H:%M:%S.%f",
                    )?
                    .timestamp()
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
                    member_id: current_member.id.clone(),
                    related_subject: HashSet::new(),
                };
                if schema.is_valid(&input) {
                    valid_properties[i] = true;
                }
            });

        // the current member is materialized if it is complete
        if valid_properties == full_property_valid {
            tx_member.send(current_member.clone()).unwrap();
            current_member = Member::new(n_properties);
            valid_properties = create_empy_valid_property();
            n_member_parsed += 1;
            if n_member_parsed % notice_frequency == 0 {
                println!(
                    "--- {:} out of {:} ({:?}%)---",
                    n_member_parsed,
                    data_injection_config.n_members,
                    (n_member_parsed as f32 / data_injection_config.n_members as f32) * 100f32
                );
            }
        }
        Ok(())
    };

    // we clone the values because we have to move them inside the thread
    let highest_date = data_injection_config.highest_date.timestamp();
    let lowest_date = data_injection_config.lowest_date.timestamp();
    let server_address = data_injection_config.server_address.clone();
    let date_field = data_injection_config.date_field.clone();
    let add_to_the_fragmentation = move || {
        handle.block_on(async {
            let mut fragmentation = LinkedListFragmentation::new(
                n_fragments,
                max_cache_element,
                &out_path,
                highest_date,
                lowest_date,
                server_address,
                date_field,
            )
            .await;
            let mut member_queue: VecDeque<Member> =
                VecDeque::with_capacity(fragmentation.max_size_cache());
            loop {
                if let Ok(member) = rx_member.recv() {
                    member_queue.push_front(member);
                } else {
                    for member in member_queue.iter() {
                        fragmentation.insert(member).await;
                    }
                    fragmentation.finalize().await;
                    break;
                }
                if let Some(member) = member_queue.pop_back() {
                    fragmentation.insert(&member).await;
                }
            }
        });
    };

    let worker = thread::spawn(add_to_the_fragmentation);

    if large_file {
        rio_turtle::TurtleParser::new(BufReader::new(file), None).parse_all(parsing_function)?;
    } else {
        rio_turtle::TurtleParser::new(read_to_string(data_dump_path)?.as_str().as_ref(), None)
            .parse_all(parsing_function)?;
    };
    std::mem::drop(tx_member);
    worker.join().unwrap();

    Ok(())
}
