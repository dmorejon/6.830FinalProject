use std::error::Error;
use std::fs::File;
// use serde::Deserialize;
use std::str::FromStr;
use csv::StringRecord;
use core::fmt::Debug;

// #[derive(Debug, Deserialize)]
// struct DummyTuple {
//     col1: i16,
//     col2: i16,
//     col3: i16,
// }

// pub fn example(file_path: &str) -> Result<(), Box<dyn Error>> {
//   let file = File::open(file_path)?;
//   let mut rdr = csv::Reader::from_reader(file);
//   for result in rdr.deserialize() {
//       // Notice that we need to provide a type hint for automatic
//       // deserialization.
//       let record: DummyTuple = result?;
//       println!("{:?}", record);
//   }
//   Ok(())
// }

pub fn fetch_records<T>(file_path: &str) -> Result<Vec<Vec<T>>, Box<dyn Error>> where T: Clone + FromStr + Debug, <T as FromStr>::Err: Debug {
  // Table to hold results
  let mut raw_table: Vec<Vec<T>> = Vec::new();

  // File contain on-disk table
  let file = File::open(file_path)?;

  // Create CSV reader object
  let mut rdr = csv::Reader::from_reader(file);

  for result in rdr.records() {
      // Holder of slots for the record
      let mut raw_record: Vec<T> = Vec::new();
      
      // Parse individual record fields as Ts
      let string_record: StringRecord = result?;
      for string_field in string_record.iter() {
        match string_field.parse::<T>() {
          Ok(parsed_field) => {
            // Case: success ==> push to our record
            raw_record.push(parsed_field);
          },
          Err(error) => {
            // Case: failure ==> die
            panic!("Something went wrong in parsing {:?}", error)
          }
        }
      }

      // Add record to the table
      raw_table.push(raw_record);
  }

  Ok(raw_table)
}