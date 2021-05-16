use std::collections::HashSet;
extern crate joinlib;
use joinlib::record::Record;

use csv::Writer;
use rand::{Rng, prelude::ThreadRng};
use rand::{seq::SliceRandom, thread_rng};

struct MissingValuePicker {
	values: HashSet<i32>,
	rng: ThreadRng,
}

impl MissingValuePicker {
	fn new(values: HashSet<i32>) -> MissingValuePicker {
		MissingValuePicker {
			values,
			rng: rand::thread_rng(),
		}
	}

	fn next(&mut self) -> i32 {
		// Keep generating random numbers uniformly until we've
		// not seen the new generated value before
		let mut v = self.rng.gen_range(i32::MIN+1..=i32::MAX);
		while self.values.contains(&v) {
			println!("MVP collision!");
			v = self.rng.gen_range(i32::MIN+1..=i32::MAX);
		}

		// Assert that we've indeed added this value to the values
		// we've seen so far. This way, subsequent calls to next()
		// will ensure to never generate this value again
		assert!(self.values.insert(v));
		
		v
	}
}

pub fn generate_table(num_rows: usize, num_cols: usize) -> Vec<Record> {
	// use a random generator to generate values
	let mut table: Vec<Record> = Vec::with_capacity(num_rows);
	let mut rng = rand::thread_rng();
	
	for _row in 0..num_rows {
		let mut fields: Vec<i32> = vec![0; num_cols];
		for col in 0..num_cols {
			// In order to do .abs() we cannot have i32::MIN in the table
			let field = rng.gen_range(i32::MIN+1..=i32::MAX);
			fields[col] = field;
		}
		let record = Record::new(fields.as_slice());
		table.push(record);
	}
	assert!(table.len() == num_rows);
	assert!(table[0].get_num_columns() == num_cols);
	table
}

pub fn write_table(table: &Vec<Record>, path: &str) -> () {
	let mut writer = Writer::from_path(path).unwrap();
	let num_cols: usize = table.get(0).unwrap().get_num_columns();
	
	// Write header
	let mut header: Vec<String> = Vec::with_capacity(num_cols);
	for col in 1..=num_cols {
		let mut column_name: String = "col".to_owned();
		column_name.push_str(&col.to_string());
		header.push(column_name);
	}
	match writer.write_record(header) {
		Err(e) => panic!("Oh no {:?}", e),
		Ok(_) => {}
	}
	writer.flush().unwrap();

	// Write records
	for r in table {
		let column_values: Vec<String> = r.get_column_values().into_iter().map(i32::to_string).collect();
		match writer.write_record(&column_values) {
			Err(e) => panic!("Oh no {:?}", e),
			Ok(_) => {}
		}
	}
	writer.flush().unwrap();
}

pub fn generate_and_write_table(num_rows: usize, num_cols: usize, path: &str) -> () {
	let table: Vec<Record> = generate_table(num_rows, num_cols);
	write_table(&table, path);
}

// given a left table, make new table based on following:
// left table: Vec<Record>
// num_rows: i32
// num_cols: i32
// join selectivity: f32

// helper data structure: missing_value_picker(target_col_values)
// which takes in a sorted list of numbers
// and exposes a next() which returns a number that is guaranteed
// to not be in the inputted list

// iterate over left and get a sorted list of the target_col_values
// matches = selectivity * (num_left_rows * num_right_rows)
//		is the number of records we wish to produce in the right table
//		which will satisfy the join. There will be other records
// create matches # of rows in the right table by randomly choose from target_col_values
// for remaining rows, get value from missing_value_picker(target_col_values) and use that as col value

fn get_table_column_values(table: &Vec<Record>, col: usize) -> HashSet<i32> {
	let mut cols = HashSet::new();
	for record in table {
		let val = *record.get_column(col);
		cols.insert(val);
	}
	cols
}

pub fn generate_right_table(left_table: Vec<Record>, 
														num_rows: usize, 
														num_cols: usize, 
														selectivity: f64,
														left_col: usize,
														right_col: usize) -> Vec<Record> {
	assert!((left_table.len() as f64) * selectivity <= num_rows as f64);

  // Normalize the number of records that the join should produce
	let matches: f64 = (left_table.len() as f64) * selectivity;
  let rounded_matches: usize = matches.floor() as usize; // TODO problem due to precision
	println!("rounded matches: {:?}", rounded_matches);

	// Assert num_rows >= matches
	assert!(num_rows as f64 >= matches);
	assert!(num_rows >= rounded_matches);

  // Construct the missing value picker
	let left_col_set: HashSet<i32> = get_table_column_values(&left_table, left_col);
	let mut mvp = MissingValuePicker::new(left_col_set);
	
  // Generate a random right table
	let mut right_table: Vec<Record> = generate_table(num_rows, num_cols);

  // Fill in enough matching values in the right table join column
  // to achieve the desired level of selectivity
  for i in 0..rounded_matches {
    let value: i32 = *left_table.get(i).unwrap().get_column(left_col);
    let right_record: &mut Record = &mut right_table[i];
    right_record.set_column(right_col, value);
		assert!(*right_table.get(i).unwrap().get_column(right_col) == value);
  }

	// Now fill in the remaing values from the missing value picker
	for i in rounded_matches..num_rows {
    // Set the join column value on the right table
    // to be some value not in the left table join column
    let value: i32 = mvp.next();
		let right_record: &mut Record = &mut right_table[i];
    right_record.set_column(right_col, value);
		assert!(*right_table.get(i).unwrap().get_column(right_col) == value);
	}

  // Rerandomize right table so join results are not just
  // at the top of the table
	right_table.shuffle(&mut thread_rng());
	
	right_table
}