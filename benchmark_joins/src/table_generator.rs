use std::collections::HashSet;
use crate::record::Record;

use csv::Writer;
use rand::Rng;


pub fn generate_left_table(num_rows: usize, num_cols: usize) -> Vec<Record> {
	// use a random generator to generate values
	let mut table: Vec<Record> = Vec::with_capacity(num_rows);
	let mut rng = rand::thread_rng();
	
	for row in 0..num_rows {
		let mut fields: Vec<i32> = Vec::with_capacity(num_cols); 
		for col in 0..num_cols {
			let field = rng.gen::<i32>();
			fields[col] = field;
		}
		let record = Record::new(fields.as_slice());
		table[row] = record;
	}

	table
}

pub fn write_table(table: Vec<Record>, path: &str) -> () {
	let mut writer = Writer::from_path(path).unwrap();
	let num_cols: usize = table.get(0).unwrap().get_num_columns();
	
	// Write header
	let mut header: Vec<String> = Vec::with_capacity(num_cols);
	for col in 1..num_cols {
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
	let table: Vec<Record> = generate_left_table(num_rows, num_cols);
	write_table(table, path);
}

struct MissingValuePicker {
	values: Vec<i32>,
	v: i32,
}

impl MissingValuePicker {
	fn new(values: HashSet<i32>) -> MissingValuePicker {
		MissingValuePicker {
			values,
			v: i32::MIN,
		}
	}

	fn next(&mut self) -> i32 {
		while (self.values.contains(self.v)) {
			// Increment current value v until 
			// it is not found in the values
			self.v += 1;
		}
		self.v
	}
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

fn get_table_column_values(table: Vec<Record>, col: usize) -> HashSet<i32> {
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
	assert!((left_table.len() as f64) * selectivity >= num_rows as f64);

	// Assert num_rows >= matches

	let matches = (left_table.len() as f64) * selectivity;

	let left_col_set = get_table_column_values(left_table, left_col);
	let mvp = MissingValuePicker::new(left_col_set);
	
	let mut table: Vec<Record> = generate_left_table(num_rows, num_cols);
	let mut rng = rand::thread_rng();

	// iterate through 0..matches 

	// for row in 0..num_rows {
	// 	let mut fields: Vec<i32> = Vec::with_capacity(num_cols); 
	// 	for col in 0..num_cols {
	// 		let field = rng.gen::<i32>();
	// 		fields[col] = field;
	// 	}
	// 	let record = Record::new(fields.as_slice());
	// 	table[row] = record;
	// }

	// Now fetch missing values
	for _ in 0..(num_rows - matches - 1) {
		table.get()
	}

	table
}