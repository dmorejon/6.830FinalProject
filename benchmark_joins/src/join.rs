use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use strum_macros::EnumIter;

use crate::record::Record;
use crate::table::SimpleTable;

#[derive(EnumIter, Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum JoinAlgos {
  NLJoin,
  BNLJoin,
  SimpleHashJoin,
}


pub struct NestedLoopsJoin<'a> {
  left: &'a mut SimpleTable,
  right: &'a mut SimpleTable,
}


impl<'a> NestedLoopsJoin<'a> {
  
  pub fn new(left: &'a mut SimpleTable, right: &'a mut SimpleTable) -> Self {
    Self {
      left,
      right
    }
  }

  pub fn equi_join(&mut self, left_col: usize, right_col: usize) -> Vec<Record> {
    // TODO: Remove the duplicated concatenation
    let mut join_result: Vec<Record> = Vec::new();

    let left_num_records: usize = self.left.get_num_records();
    let right_num_records: usize = self.right.get_num_records();

    for _l in 0..left_num_records {
      let mut left_record: Record = self.left.read_next_record().clone();

      for _r in 0..right_num_records {
        let mut right_record: Record = self.right.read_next_record();

        if left_record.get_column(left_col) == right_record.get_column(right_col) {
          // Join condition is met ==> new record 
          let join_record: Record = Record::merge(&mut left_record, &mut right_record);
          join_result.push(join_record);
        }
      }
      self.right.rewind();
    }
    self.left.rewind();
    join_result
  }
}


pub struct BlockNL<'a> {
  left: &'a mut SimpleTable,
  right: &'a mut SimpleTable,
  l_block_sz: usize,
  r_block_sz: usize,
}

impl<'a> BlockNL<'a> {
  
  pub fn new(left: &'a mut SimpleTable, right: &'a mut SimpleTable, l_block_sz: usize, r_block_sz: usize) -> Self {
    Self {
      left,
      right,
      l_block_sz,
      r_block_sz,
    }
  }

  pub fn equi_join(&mut self, left_col: usize, right_col: usize) -> Vec<Record> {
    // TODO: Remove the duplicated concatenation
    let mut join_result: Vec<Record> = Vec::new();

    let left_num_blocks: f64 = ((self.left.get_num_records() as f64) / (self.l_block_sz as f64)).ceil();
    let right_num_blocks: f64 = ((self.right.get_num_records() as f64) / (self.r_block_sz as f64)).ceil();

    for _l in 0..(left_num_blocks as usize) {
      let left_block = self.left.read_next_block(self.l_block_sz);

      for _r in 0..(right_num_blocks as usize){
        let right_block = self.right.read_next_block(self.r_block_sz);

        for mut left_record in left_block {
          for mut right_record in right_block {
            if left_record.get_column(left_col) == right_record.get_column(right_col) {
              // Join condition is met ==> new record 
              let join_record: Record = Record::merge(&mut left_record, &mut right_record);
              join_result.push(join_record);
            }
          }
        }
      }
      self.right.rewind();
    }
    self.left.rewind();
    join_result
  }

  pub fn get_left_block_size(&self) -> usize {
    return self.l_block_sz;
  }

  pub fn get_right_block_size(&self) -> usize {
    return self.r_block_sz;
  }
}

pub struct SimpleHashJoin<'a> {
  left: &'a mut SimpleTable,
  right: &'a mut SimpleTable,
}

impl<'a> SimpleHashJoin<'a> {
  
  pub fn new(left: &'a mut SimpleTable, right: &'a mut SimpleTable) -> Self {
    Self {
      left,
      right
    }
  }

  pub fn equi_join(&mut self, left_col: usize, right_col: usize) -> Vec<Record> {
    // TODO: Remove the duplicated concatenation
    let mut join_result: Vec<Record> = Vec::new();
    let mut hashtable: HashMap<i32, Vec<Record>> = HashMap::new();

    let left_num_records: usize = self.left.get_num_records();
    let right_num_records: usize = self.right.get_num_records();

    for _l in 0..right_num_records {
      let right_record: Record = self.right.read_next_record().clone();
      hashtable.entry(right_record.get_column(right_col)).or_insert(Vec::new()).push(right_record);
    }
    self.right.rewind();

    for _r in 0..left_num_records {
      let mut left_record: Record = self.left.read_next_record().clone();
      let matches = match hashtable.get(&left_record.get_column(left_col)) {
        None => continue,
        Some(v) => v
      };
      for record in matches {
        let join_record: Record = Record::merge(&mut left_record, &mut record.clone());
          join_result.push(join_record);
      }
    }
    self.left.rewind();
    join_result
  }
}
