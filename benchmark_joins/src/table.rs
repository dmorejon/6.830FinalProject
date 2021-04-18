use core::str::FromStr;
use core::fmt::Debug;
use std::clone::Clone;

use crate::record::Record;
use crate::readtable::fetch_records;

pub struct SimpleTable<T> {
  records: Vec<Record<T>>,
  index: usize,

}

impl<T> SimpleTable<T> where T: Clone + Debug + FromStr, <T as FromStr>::Err: Debug {
  pub fn new(filepath: &str) -> SimpleTable<T> {
    // Make the object
    let mut simple_table: SimpleTable<T> = SimpleTable {
      records: Vec::new(),
      index: 0
    };

    // Get raw table contents from on-disk table
    let raw_table: Vec<Vec<T>> = fetch_records::<T>(filepath).unwrap();

    for raw_record in raw_table.iter() {
      // Add each raw record as proper record to the table
      let record: Record<T> = Record::new(raw_record);
      simple_table.records.push(record);
    }

    simple_table
  }

  pub fn get_num_records(&self) -> usize{
    return self.records.len();
  }

  pub fn read_next_record(&mut self) -> Record<T> {
    let record = self.records[self.index].clone();
    self.index += 1;
    record.clone()
  }
}