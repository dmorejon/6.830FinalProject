use std::error::Error;
use std::fs::File;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct DummyTuple {
    col1: i16,
    col2: i16,
    col3: i16,
}

pub fn example(file_path: &str) -> Result<(), Box<dyn Error>> {
  let file = File::open(file_path)?;
  let mut rdr = csv::Reader::from_reader(file);
  for result in rdr.deserialize() {
      // Notice that we need to provide a type hint for automatic
      // deserialization.
      let record: DummyTuple = result?;
      println!("{:?}", record);
  }
  Ok(())
}

