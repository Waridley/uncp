//! Query interface for accessing data and relations

use polars::prelude::*;

use crate::data::{RelationStore, ScanState};

#[derive(Debug)]
pub struct Query<'a> {
    pub data: &'a DataFrame,
    pub relations: &'a RelationStore,
}

impl<'a> Query<'a> {
    pub fn new(state: &'a ScanState, relations: &'a RelationStore) -> Self {
        Self {
            data: &state.data,
            relations,
        }
    }

    pub fn files_by_type(&self, file_type: &str) -> PolarsResult<DataFrame> {
        self.data
            .clone()
            .lazy()
            .filter(col("file_type").eq(lit(file_type)))
            .collect()
    }

    pub fn files_needing_hashing(&self) -> PolarsResult<DataFrame> {
        self.data
            .clone()
            .lazy()
            .filter(col("hashed").eq(lit(false)))
            .collect()
    }
}
