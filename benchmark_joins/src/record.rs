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

#[derive(Debug)]
pub struct Record<T> {
  data: Vec<T>
}

impl<T> Record<T> where T: Clone + Debug {
  pub fn new(input_record: &[T]) -> Record<T> {
    Record {
      data: input_record.to_vec()
    }
  }

  pub fn get_column(&self, i: usize) -> &T {
    &self.data[i]
  }

  pub fn get_num_columns(&self) -> usize {
    self.data.len()
  } 
}