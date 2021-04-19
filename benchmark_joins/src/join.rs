use crate::record::Record;
use crate::SimpleTable;

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
}

impl<'a> BlockNL<'a> {
  
  pub fn new(left: &'a mut SimpleTable, right: &'a mut SimpleTable) -> Self {
    Self {
      left,
      right
    }
  }

  pub fn equi_join(&mut self, left_col: usize, right_col: usize, l_block_sz: usize, r_block_sz: usize) -> Vec<Record> {
    // TODO: Remove the duplicated concatenation
    let mut join_result: Vec<Record> = Vec::new();

    let left_num_blocks: usize = self.left.get_num_records() / l_block_sz;
    let right_num_blocks: usize = self.right.get_num_records() / r_block_sz;

    for _l in 0..left_num_blocks {
      let left_block = self.left.read_next_block(l_block_sz);

      for _r in 0..right_num_blocks {
        let right_block = self.right.read_next_block(r_block_sz);

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
}