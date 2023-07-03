use std::fmt;

#[derive(Default, Debug, Clone,PartialEq, Eq, Hash)]
pub struct Member {
    pub properties: Vec<String>,
    pub date: i64,
    pub id: String,
}

impl Member{
    pub fn new(n_properties: usize) -> Self{
        Self{
            properties: Vec::with_capacity(n_properties),
            ..Default::default()
        }
    }
}

impl fmt::Display for Member{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut resp = String::new();
        for m in self.properties.iter(){
            resp.push_str(m.as_str());
            resp.push_str("\n");
        }
        write!(f, "{}", resp)
    }
}