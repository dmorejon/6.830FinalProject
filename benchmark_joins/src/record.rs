use std::clone::Clone;
use std::fmt::Debug;

#[derive(Debug, Clone)]
pub struct Record {
  fields: Vec<i32>
}

impl Record where{
  pub fn new(input_record: &[i32]) -> Record {
    Record {
      fields: input_record.to_vec()
    }
  }

  pub fn merge(r1: &Record, r2: &Record) -> Record {
    // Combine the record fields
    let mut combined_fields = r1.fields.clone();
    let mut fields2 = r2.fields.clone();
    combined_fields.append(&mut fields2);

    // New record from combined fields
    Record::new(combined_fields.as_slice())
  }

  pub fn get_column(&self, i: usize) -> &i32 {
    &self.fields[i]
  }

  pub fn get_num_columns(&self) -> usize {
    self.fields.len()
  } 
}