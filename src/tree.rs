use derive_getters;
use derive_new;
use rio_api;
use std::fmt;

#[derive(Clone, PartialEq, Eq, Debug)]
/// The type of the relationship.
/// https://treecg.github.io/specification/#vocabulary
pub enum RelationOperator {
    #[allow(dead_code)]
    /// All elements in the related node have this prefix.
    PrefixRelation,
    #[allow(dead_code)]
    /// All elements in the related node have this substring.
    SubstringRelation,
    #[allow(dead_code)]
    /// All members of this related node end with this suffix.
    SuffixRelation,
    #[allow(dead_code)]
    /// The related Node’s members are greater than the value. For string comparison,
    /// this relation can refer to a comparison configuration.
    GreaterThanRelation,

    /// Similar to GreaterThanRelation.
    GreaterThanOrEqualToRelation,

    #[allow(dead_code)]
    /// Similar to GreaterThanRelation.
    LessThanRelation,
    /// Similar to GreaterThanRelation.
    LessThanOrEqualToRelation,

    #[allow(dead_code)]
    /// Similar to GreaterThanRelation.
    EqualThanRelation,

    #[allow(dead_code)]
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
    path: String,
    /// The contextual value of this node.
    value: String,
    /// Link to the TREE node document for this relationship.
    node: String,
    /// The type of the relationship.
    relation_type: RelationOperator,
    /// The Node containing the relation
    current_node_iri: String,
    /// the id of the blank node
    relation_id: String,
}

impl fmt::Display for Relation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut resp = String::new();
        let relation_comparator = self.relation_type.to_string();

        let triples = [
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
                predicate: rio_api::model::NamedNode { iri: TYPE_VOCAB },
                object: rio_api::model::NamedNode {
                    iri: &relation_comparator,
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
            rio_api::model::Triple {
                subject: rio_api::model::BlankNode {
                    id: &self.relation_id,
                }
                .into(),
                predicate: rio_api::model::NamedNode {
                    iri: TREE_VALUE_VOCAB,
                },
                object: rio_api::model::Literal::Typed {
                    value: &self.value,
                    datatype: rio_api::model::NamedNode {
                        iri: DATA_TIME_VOCAB,
                    },
                }
                .into(),
            },
            rio_api::model::Triple {
                subject: rio_api::model::BlankNode {
                    id: &self.relation_id,
                }
                .into(),
                predicate: rio_api::model::NamedNode {
                    iri: TREE_PATH_VOCAB,
                },
                object: rio_api::model::NamedNode { iri: &self.path }.into(),
            },
        ];

        for triple in triples {
            resp.push_str(&triple.to_string());
            resp.push_str(".\n");
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
