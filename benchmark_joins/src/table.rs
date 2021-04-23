use std::clone::Clone;
use std::cmp::min;
use crate::record::Record;
use crate::readtable::fetch_records;

pub struct SimpleTable {
  records: Vec<Record>,
  index: usize,

}

impl SimpleTable {
  pub fn new(filepath: &str) -> SimpleTable {
    // Make the object
    let mut simple_table: SimpleTable = SimpleTable {
      records: Vec::new(),
      index: 0
    };

    // Get raw table contents from on-disk table
    let raw_table: Vec<Vec<i32>>;
    match fetch_records(filepath) {
      Err(e) => panic!("{:?}", e),
      Ok(fetched_raw_table) => {
        raw_table = fetched_raw_table;
      }
    }
    for raw_record in raw_table.iter() {
      // Add each raw record as proper record to the table
      let record: Record = Record::new(raw_record);
      simple_table.records.push(record);
    }

    simple_table
  }

  pub fn copy_to_vec_of_records(&self) -> Vec<Record> {
    self.records.clone()
  }

  pub fn get_num_records(&self) -> usize{
    return self.records.len();
  }

  pub fn get_num_columns_per_record(&self) -> usize{
    return self.records.get(0).unwrap().get_num_columns();
  }

  pub fn read_next_record(&mut self) -> Record {
    let record = self.records[self.index].clone();
    self.index += 1;
    record.clone()
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