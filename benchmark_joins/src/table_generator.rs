use std::clone::Clone;
use std::cmp::min;
use crate::record::Record;
use crate::readtable::fetch_records;

pub struct Generated_Table extends SimpleTable{
    //design - might not be correct
    //extremely basic table with no options except size
}

impl TableGen {
  pub fn new(filepath: &str) -> TableGenerator {
    // Make the object
    let mut result_table: Table 
    let mut table_generator: TableGenerator = TableGenerator {
      records: Vec::new(),
      index: 0
    };

    // // Get raw table contents from on-disk table
    // let raw_table: Vec<Vec<i32>>;
    // match fetch_records(filepath) {
    //   Err(e) => panic!("{:?}", e),
    //   Ok(fetched_raw_table) => {
    //     raw_table = fetched_raw_table;
    //   }
    // }
//     for raw_record in raw_table.iter() {
//       // Add each raw record as proper record to the table
//       let record: Record = Record::new(raw_record);
//       simple_table.records.push(record);
//     }

//     simple_table
//   }

//todo
//   pub fn get_num_records(&self) -> usize{
//     return self.records.len();
//   }

//   pub fn get_num_columns_per_record(&self) -> usize{
//     return self.records.get(0).unwrap().get_num_columns();
//   }

//   pub fn read_next_record(&mut self) -> Record {
//     let record = self.records[self.index].clone();
//     self.index += 1;
//     record.clone()
//   }

//   pub fn read_next_block(&mut self, block_sz: usize) -> &[Record] {
//     let end_index = min(self.index+block_sz, self.records.len());
//     let block = &self.records[self.index..end_index];
//     self.index = end_index;
//     block
//   }

//   pub fn rewind(&mut self) {
//     self.index = 0;
//   }

  pub fn generate_table(&self, int36 lowerBound, int36 upperBound) -> Table {

    // use a random generator to generate values

    cols = self.get_num_columns
    rows = self.get_num_columns_per_record

    let mut rng = rand::thread_rng();
    //println!("Integer: {}", rng.gen_range(lowerBound..upperBound));
    result = Vec::new()

    let mut xind = 0;
    let mut yind = 0;
        for n in 0..cols {
            let intermediate_vector = Vec::new()
            for n in 0..rows {
                intermediate_vector.push(rng.gen_range(lowerBound..upperBound)
            }
            result.push(intermediate_vector)
        }

        
        result
  }
}