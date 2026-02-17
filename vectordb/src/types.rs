use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VectorRecord {
    pub id: u64,
    pub values: Vec<f32>,
    pub version: u32,
    pub deleted: bool,
}

impl VectorRecord {
    pub fn new(id: u64, values: Vec<f32>) -> Self {
        Self {
            id,
            values,
            version: 0,
            deleted: false,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Neighbor {
    pub id: u64,
    pub distance: f32,
}

pub trait VectorIndex {
    fn upsert(&mut self, id: u64, vector: Vec<f32>);
    fn delete(&mut self, id: u64) -> bool;
    fn search(&self, query: &[f32], k: usize) -> Vec<Neighbor>;
    fn len(&self) -> usize;
}
