use std::clone::Clone;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::panic;

// Maximum fields in a record
pub const M: usize = 100;

#[derive(Debug, Clone)]
pub struct Record {
	// Array of i32s whose capacity
	// is preallocated to max of M
	fields: [i32; M],

	// The next index to insert 
	// a new column value into
	tail: usize,
}

impl Record {
	pub fn new(input_record: &[i32]) -> Record {
		// New empty record
		let mut new_record: Record = Record {
			fields: [0; M],
			tail: 0
		};
		
		// Add each input column
		for value_ptr in input_record.into_iter() {
			new_record.add_column(*value_ptr);
		}

		new_record
	}

	pub fn merge(r1: &Record, r2: &Record) -> Record {
		// New record
		let mut nr: Record = Record {
			fields: [0; M],
			tail: 0
		};

		// Add first record
		for i in 0..r1.get_num_columns() {
			nr.add_column(r1.get_column(i));
		}

		// Add second record
		for i in 0..r2.get_num_columns() {
			nr.add_column(r2.get_column(i));
		}

		nr
	}

	pub fn get_column(&self, i: usize) -> i32 {
		// Capacity OOB
		if i >= M {
			panic!("OOB Capacity");
		}
		// Index OOB
		if i >= self.tail {
			panic!("OOB Index")
		}

		self.fields[i].clone()
	}

	pub fn set_column(&mut self, i: usize, value: i32) -> () {
		// Capacity OOB
		if i >= M {
			panic!("OOB Capacity");
		}
		// Index OOB
		if i >= self.tail {
			panic!("OOB Index")
		}

		self.fields[i] = value;
	}

	pub fn get_column_values(&self) -> Vec<&i32> {
		// self.fields.to_vec()
		let mut fields = Vec::with_capacity(self.get_num_columns());
		for field in &self.fields[0..self.tail] {
			fields.push(field);
		}
		fields
	}

	pub fn get_num_columns(&self) -> usize {
		self.tail
	}

	fn add_column(&mut self, value: i32) -> () {
		// Check capacity to insert
		if (self.tail as usize) >= M {
			panic!("Insufficient capacity for insert, tail: {:?}, M: {:?}", self.tail, M);
		} 
		// Insert column and move to next spot placeholder
		self.fields[self.tail] = value;
		self.tail += 1;
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
	use rand::{seq::SliceRandom, thread_rng};

	fn check_size(r: &Record, expected_size: usize) {
		assert_eq!(expected_size, r.get_num_columns());
	}

	fn check_column(r: &Record, col_idx: usize, expected_value: i32) {
		assert_eq!(expected_value, r.get_column(col_idx));
	}

	fn check_columns_in_order(r: &Record, columns: &[i32]) {
		for i in 0..columns.len() {
			check_column(r, i, columns[i]);
		}
	}
	
	fn check_columns_randomly(r: &Record, columns: &[i32]) {
		// Populate indexs
		let mut indexes: Vec<usize> = (0..columns.len()).collect();

		// Shuffle indexes
		indexes.shuffle(&mut thread_rng());

		// Check column values
		for i in indexes {
			check_column(r, i, columns[i])
		}
	}

	fn check_column_oob(r: &Record) {
		// try idx len, should panic
		let result = panic::catch_unwind(|| r.get_column(r.get_num_columns()));
		assert!(result.is_err());

		// try idx len + 100, should panic
		let result = panic::catch_unwind(|| r.get_column(100 + r.get_num_columns()));
		assert!(result.is_err());
	}

	fn check_record(r: &Record, columns: &[i32]) {
		// Check size
		check_size(r, columns.len());

		// Check column values
		check_columns_in_order(r, columns);
		check_columns_randomly(r, columns);

		// Check OOB access
		check_column_oob(r);
	}

	#[test]
	fn test_empty_record() {
		// Tiny
		let columns = [];
		let r = Record::new(&columns);
		check_record(&r, &columns);
	}

	#[test]
	fn test_small_record() {
		// Smol
		let columns = [98752, -23765];
		let r = Record::new(&columns);
		check_record(&r, &columns);
	}

	#[test]
	fn test_medium_record() {
		// Medium
		let columns = [1, 2, 3, 4, 4, 4, 5, 6, 420];
		let r = Record::new(&columns);
		check_record(&r, &columns);
	}

	#[test]
	fn test_large_record() {
		// Thiqq
		let mut columns: [i32; 75] = [0; 75];
		for v in 1000..1075 {
			columns[v - 1000] = v as i32;
		}
		let r = Record::new(&columns);
		check_record(&r, &columns);
	}

	#[test]
	fn test_max_record() {
		// Huge
		let mut columns: [i32; M] = [0; M];
		for v in 6..(6+M) {
			columns[v - 6] = v as i32;
		}
		let r = Record::new(&columns);
		check_record(&r, &columns);
	}
}