use crate::record;

pub struct SimpleTable<const SIZE: usize> {
  pub records:[record::Record; SIZE],
  pub index:usize, //TODO: make this private?

}

impl<const SIZE: usize> SimpleTable<SIZE> {
  pub fn new(filepath: &str) -> Self {
    // TODO: make stuff from file
    // read from file, make array? 
    // make Self
    //return Self{};
    return Self{[], 0};
  }

  pub fn get_num_records(&self) -> usize{
    return SIZE;
  }

  pub fn read_next_record(&self) -> &Record {
    let record = records[self.index];
    self.index += 1;
    record;
  }
}