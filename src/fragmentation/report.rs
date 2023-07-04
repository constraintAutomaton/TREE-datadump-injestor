use serde;
use super::fragment::Boundary;

#[derive(serde::Serialize)]
pub struct Report{
    pub n_member: Option<usize>,
    pub boundary: Boundary
}