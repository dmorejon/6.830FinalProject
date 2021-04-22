use std::clone::Clone;
use std::cmp::Ordering;
use std::fmt::Debug;

#[derive(Debug, Clone)]
pub struct Record {
  fields: Vec<i32>
}


impl Record {
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

impl Ord for Record {
  fn cmp(&self, other: &Self) -> Ordering {
      self.fields.cmp(&other.fields)
  }
}

impl PartialOrd for Record {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
      Some(self.fields.cmp(&other.fields))
  }
}

impl PartialEq for Record {
  fn eq(&self, other: &Self) -> bool {
      self.fields == other.fields
  }
}

impl Eq for Record {}

#[cfg(test)]
mod tests {
  use super::*;
  
  #[test]
  fn test_record() {
    let data = [1, 2, 3, 420];
    let rec = Record::new(&data);
    assert_eq!(420, *rec.get_column(3));
    assert_eq!(4, rec.get_num_columns());
  }
}