use std::clone::Clone;
use std::fmt::Debug;

// pub struct Record<T, const LENGTH: usize> {
//   pub data:[T; LENGTH],

// }

// impl<T, const LENGTH: usize> Record<T, LENGTH> {
//   //TODO: see how to construct, if it is ugly then 
//   //      write a "new()" method or something
//   pub fn get_column(&self, i: usize) -> &T {
//     &self.data[i]
//   }

//   pub fn get_num_columns(&self) -> usize {
//     LENGTH
//   } 
// }

#[derive(Debug, Clone)]
pub struct Record<T> {
  fields: Vec<T>
}

impl<T> Record<T> where T: Clone + Debug {
  pub fn new(input_record: &[T]) -> Record<T> {
    Record {
      fields: input_record.to_vec()
    }
  }

  pub fn merge(r1: &Record<T>, r2: &Record<T>) -> Record<T> {
    // Combine the record fields
    let mut combined_fields = r1.fields.clone();
    let mut fields2 = r2.fields.clone();
    combined_fields.append(&mut fields2);

    // New record from combined fields
    Record::new(combined_fields.as_slice())
  }

  pub fn get_column(&self, i: usize) -> &T {
    &self.fields[i]
  }

  pub fn get_num_columns(&self) -> usize {
    self.fields.len()
  } 
}