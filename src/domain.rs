#[derive(Default, Debug, Clone)]
pub struct Members {
    pub properties: Vec<String>,
    pub date: String,
    pub id: String,
}

impl Members{
    pub fn new(n_properties: usize) -> Self{
        Self{
            properties: Vec::with_capacity(n_properties),
            ..Default::default()
        }
    }
}