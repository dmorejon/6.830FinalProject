use std::error::Error;
use std::fs::File;
use csv::StringRecord;

pub fn fetch_records(file_path: &str) -> Result<Vec<Vec<i32>>, Box<dyn Error>> {
  // Table to hold results
  let mut raw_table: Vec<Vec<i32>> = Vec::new();

  // File contain on-disk table
  let file = File::open(file_path)?;

  // Create CSV reader object
  let mut rdr = csv::Reader::from_reader(file);

  for result in rdr.records() {
      // Holder of slots for the record
      let mut raw_record: Vec<i32> = Vec::new();
      
      // Parse individual record fields as Ts
      let string_record: StringRecord = result?;
      for string_field in string_record.iter() {
        match string_field.parse::<i32>() {
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