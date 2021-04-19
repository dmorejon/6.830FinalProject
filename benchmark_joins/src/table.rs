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

  pub fn get_num_records(&self) -> usize{
    return self.records.len();
  }

  pub fn read_next_record(&mut self) -> Record {
    let record = self.records[self.index].clone();
    self.index += 1;
    record.clone()
  }

  pub fn rewind(&mut self) {
    self.index = 0;
  }
}