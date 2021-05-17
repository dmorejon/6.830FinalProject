use std::{clone::Clone, slice::Iter};
use std::cmp::min;
use rayon::iter::IntoParallelRefIterator;
use rayon::prelude::ParallelSliceMut;

use crate::record::Record;
use crate::readtable::fetch_records;

pub struct SimpleTable {
  // The collection of records which
  // make up this table
  records: Vec<Record>,

  // Number of columns in a record
  num_columns: usize,

  // Index of the next read
  index: usize,
}

impl SimpleTable {
  pub fn new(filepath: &str) -> SimpleTable {
    // Get raw table contents from on-disk table
    let raw_table: Vec<Vec<i32>>;
    match fetch_records(filepath) {
      Err(e) => panic!("{:?}", e),
      Ok(fetched_raw_table) => {
        raw_table = fetched_raw_table;
      }
    }
    let num_columns = raw_table.get(0).unwrap().len();

    // Create Record from raw
    let records = raw_table
      .iter()
      .map(|rr| Record::new(rr))
      .collect();

    SimpleTable {
      records,
      num_columns,
      index: 0
    }
  }

  // Expensive operation
  pub fn copy_to_vec_of_records(&self) -> Vec<Record> {
    self.records.clone()
  }

  // Expensive operation
  // Sorts the records by column in parrallel
  pub fn sort_by(&mut self, i: usize) {
    // Unstable sort will be faster for us because
    // stable sorting doesn't matter and we are unlikely
    // to have almost-sorted records or a handful of sorted
    // record subvecs that will be merged
    self.records.par_sort_unstable_by_key(|r| *r.get_column(i));
  }

  pub fn get_num_records(&self) -> usize {
    self.records.len()
  }

  pub fn get_num_columns_per_record(&self) -> usize {
    self.num_columns
  }

  pub fn record_iterator(&self) -> Iter<Record> {
    self.records.iter()
  }

  pub fn record_par_iterator(&self) -> rayon::slice::Iter<Record> {
    self.records.par_iter()
  }

  pub fn record_view(&self) -> &[Record] {
    &self.records
  }
  
  pub fn read_next_record(&mut self) -> &Record {
    let r = &self.records[self.index];
    self.index += 1;
    r
  }

  pub fn read_next_block(&mut self, block_sz: usize) -> &[Record] {
    let end_index = min(self.index+block_sz, self.records.len());
    let block = &self.records[self.index..end_index];
    self.index = end_index;
    block
  }

  pub fn rewind(&mut self) {
    self.index = 0;
  }
}