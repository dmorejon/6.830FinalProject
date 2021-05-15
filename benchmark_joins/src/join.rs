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
  RadixJoin,
  PNLJoin,
  ParallelSimpleHashJoin,
  ParallelUnaryLeapFrogJoin,
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
    // Number of records in left and right tables
    let left_size = self.left.get_num_records();
    let right_size = self.right.get_num_records();

    // Since this is a primary-key foreign-key equijoin
    // we know the the join will be no larger than left table
    let mut join_result = Vec::with_capacity(left_size);

    for _l in 0..left_size {
      let left_record = self.left.read_next_record();

      for _r in 0..right_size {
        let right_record = self.right.read_next_record();

        if left_record.get_column(left_col) == right_record.get_column(right_col) {
          // Join condition is met ==> new record 
          let join_record = Record::merge(left_record, right_record);
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

  fn get_effective_num_blocks(&self, num_records: usize, block_size: usize) -> usize {
    let intermidate: f64 = ((num_records as f64) / (block_size as f64)).ceil();
    intermidate as usize
  }

  pub fn equi_join(&mut self, left_col: usize, right_col: usize) -> Vec<Record> {
    // Number of records in left and right tables
    let left_size = self.left.get_num_records();
    let right_size = self.right.get_num_records();

    // Number of effective blocks for left and right tables
    let effective_left_num_blocks = self.get_effective_num_blocks(left_size, self.l_block_sz);
    let effective_right_num_blocks = self.get_effective_num_blocks(right_size, self.r_block_sz);

    // Since this is a primary-key foreign-key equijoin
    // we know the the join will be no larger than left table
    let mut join_result = Vec::with_capacity(left_size);

    for _l in 0..effective_left_num_blocks  {
      let left_block = self.left.read_next_block(self.l_block_sz);

      for _r in 0..effective_right_num_blocks {
        let right_block = self.right.read_next_block(self.r_block_sz);

        for left_record in left_block {
          for right_record in right_block {

            if left_record.get_column(left_col) == right_record.get_column(right_col) {
              // Join condition is met ==> new record 
              let join_record = Record::merge(left_record, right_record);
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
    // Number of records in left and right tables
    let left_size = self.left.get_num_records();
    let right_size = self.right.get_num_records();

    // Since this is a primary-key foreign-key equijoin
    // we know the the join will be no larger than left table
    let mut join_result = Vec::with_capacity(left_size);

    let mut hash_table: HashMap<&i32, Vec<&Record>> = HashMap::new();

    // Get the right table's view of its records
    let right_records = self.right.record_view();
    assert!(right_size == right_records.len());

    // Now we build the hash table on the smaller table
    // since this results in the fewest operations during join
    for r in right_records {
      let right_column_value = r.get_column(right_col);

      // Insert map from the hash of right join column value the record itself
      hash_table.entry(right_column_value).or_insert(Vec::new()).push(r);
    }

    for _l in 0..left_size {
      let left_record = self.left.read_next_record();
      let left_column_value = left_record.get_column(left_col);
      
      match hash_table.get(left_column_value) {
        // If hash table doesn't have this value, 
        // we know for sure that this record does not 
        // participate in the join
        None => continue,

        // But if there are some matches for the value,
        // then we know they ALL participate in the join
        Some(right_record_matches) => {
          for right_record in right_record_matches {
            let join_record: Record = Record::merge(left_record, right_record);
            join_result.push(join_record);
          }
        }
      };
    }
    self.left.rewind();

    join_result
  }
}
