use derive_getters;
use derive_new;
use rio_api;
use std::fmt;

#[allow(dead_code)]
#[derive(Clone, PartialEq, Eq, Debug)]
/// The type of the relationship.
/// https://treecg.github.io/specification/#vocabulary
pub enum RelationOperator {
    /// All elements in the related node have this prefix.
    PrefixRelation,
    /// All elements in the related node have this substring.
    SubstringRelation,
    /// All members of this related node end with this suffix.
    SuffixRelation,
    /// The related Node’s members are greater than the value. For string comparison,
    /// this relation can refer to a comparison configuration.
    GreaterThanRelation,
    /// Similar to GreaterThanRelation.
    GreaterThanOrEqualToRelation,
    /// Similar to GreaterThanRelation.
    LessThanRelation,
    /// Similar to GreaterThanRelation.
    LessThanOrEqualToRelation,
    /// Similar to GreaterThanRelation.
    EqualThanRelation,

    /// A contains b iff no points of b lie in the exterior of a, and at least one point
    /// of the interior of b lies in the interior of a.
    /// reference http://lin-ear-th-inking.blogspot.com/2007/06/subtleties-of-ogc-covers-spatial.html
    GeospatiallyContainsRelation,
}

impl fmt::Display for RelationOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let string_representation: &str = {
            match self {
                RelationOperator::PrefixRelation => "https://w3id.org/tree#PrefixRelation",
                RelationOperator::SubstringRelation => "https://w3id.org/tree#SubstringRelation",
                RelationOperator::SuffixRelation => "https://w3id.org/tree#SuffixRelation",
                RelationOperator::GreaterThanRelation => {
                    "https://w3id.org/tree#GreaterThanRelation"
                }
                RelationOperator::GreaterThanOrEqualToRelation => {
                    "https://w3id.org/tree#GreaterThanOrEqualToRelation"
                }
                RelationOperator::LessThanRelation => "https://w3id.org/tree#LessThanRelation",
                RelationOperator::LessThanOrEqualToRelation => {
                    "https://w3id.org/tree#LessThanOrEqualToRelation"
                }
                RelationOperator::EqualThanRelation => "https://w3id.org/tree#EqualThanRelation",
                RelationOperator::GeospatiallyContainsRelation => {
                    "https://w3id.org/tree#GeospatiallyContainsRelation"
                }
            }
        };
        write!(f, "{}", string_representation)
    }
}

#[derive(derive_new::new, Clone, PartialEq, Eq, derive_getters::Getters, Debug)]
/// Represents a relationship between the members across two nodes.
pub struct Relation {
    /// A property path, as defined by SHACL, that indicates what resource the tree:value affects.
    path: Option<String>,
    /// The contextual value of this node.
    value: Option<String>,
    /// Link to the TREE node document for this relationship.
    node: String,
    /// The type of the relationship.
    relation_type: Option<RelationOperator>,
    /// The Node containing the relation
    current_node_iri: String,
    /// the id of the blank node
    relation_id: String,
}

impl Relation {
    pub fn new_unconstraint(node: String, current_node_iri: String, relation_id: String) -> Self {
        Self {
            path: None,
            value: None,
            node,
            current_node_iri,
            relation_type: None,
            relation_id,
        }
    }
}

impl fmt::Display for Relation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut resp = String::new();
        let mut triples = vec![
            rio_api::model::Triple {
                subject: rio_api::model::NamedNode {
                    iri: &self.current_node_iri,
                }
                .into(),
                predicate: rio_api::model::NamedNode {
                    iri: TREE_RELATION_VOCAB,
                },
                object: rio_api::model::BlankNode {
                    id: &self.relation_id,
                }
                .into(),
            },
            rio_api::model::Triple {
                subject: rio_api::model::BlankNode {
                    id: &self.relation_id,
                }
                .into(),
                predicate: rio_api::model::NamedNode {
                    iri: TREE_NODE_VOCAB,
                },
                object: rio_api::model::NamedNode { iri: &self.node }.into(),
            },
        ];
        let relation_comparator = self
            .relation_type
            .as_ref()
            .and_then(|v| Some(v.to_string()))
            .unwrap_or_default();
        if !relation_comparator.is_empty() {
            triples.push(rio_api::model::Triple {
                subject: rio_api::model::BlankNode {
                    id: &self.relation_id,
                }
                .into(),
                predicate: rio_api::model::NamedNode { iri: TYPE_VOCAB },
                object: rio_api::model::NamedNode {
                    iri: &relation_comparator,
                }
                .into(),
            })
        }

        let value = self
            .value
            .as_ref()
            .and_then(|v| Some(v.to_string()))
            .unwrap_or_default();
        if !value.is_empty() {
            triples.push(rio_api::model::Triple {
                subject: rio_api::model::BlankNode {
                    id: &self.relation_id,
                }
                .into(),
                predicate: rio_api::model::NamedNode {
                    iri: TREE_VALUE_VOCAB,
                },
                object: rio_api::model::Literal::Typed {
                    value: &value,
                    datatype: rio_api::model::NamedNode {
                        iri: DATA_TIME_VOCAB,
                    },
                }
                .into(),
            });
        }

        let path = self
            .path
            .as_ref()
            .and_then(|v| Some(v.to_string()))
            .unwrap_or_default();

        if !path.is_empty() {
            triples.push(rio_api::model::Triple {
                subject: rio_api::model::BlankNode {
                    id: &self.relation_id,
                }
                .into(),
                predicate: rio_api::model::NamedNode {
                    iri: TREE_PATH_VOCAB,
                },
                object: rio_api::model::NamedNode { iri: &path }.into(),
            });
        }

        for triple in triples {
            resp.push_str(&triple.to_string());
            resp.push_str(" .\n");
        }

        write!(f, "{}", resp)
    }
}

const TREE_PATH_VOCAB: &'static str = "https://w3id.org/tree#path";
const TREE_NODE_VOCAB: &'static str = "https://w3id.org/tree#node";
const TREE_VALUE_VOCAB: &'static str = "https://w3id.org/tree#value";
const TYPE_VOCAB: &'static str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const TREE_RELATION_VOCAB: &'static str = "https://w3id.org/tree#relation";
const DATA_TIME_VOCAB: &'static str = "http://www.w3.org/2001/XMLSchema#dateTime";
