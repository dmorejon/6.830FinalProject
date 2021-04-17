pub struct Record<T, const LENGTH: usize> {
  pub data:[T; LENGTH],

}

impl<T, const LENGTH: usize> Record<T, LENGTH> {
  //TODO: see how to construct, if it is ugly then 
  //      write a "new()" method or something
  pub fn get_column(&self, i: usize) -> &T {
    &self.data[i]
  }

  pub fn get_num_columns(&self) -> usize {
    LENGTH
  } 
}