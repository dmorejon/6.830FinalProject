use std::cmp::Ordering;
use std::fmt::Debug;
use std::panic;

// Maximum fields in a record
pub const M: usize = 20;

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
		// Check number of columns
		let n = input_record.len();
		if n > M {
			panic!("Record input has {:?} columns, but max support is {:?}", n, M);
		}

		// Populate fields from input record
		let mut fields: [i32; M] = [0; M];
		for (i, f) in input_record.iter().enumerate() {
			fields[i] = *f;
		}
		
		Record {
			fields,
			tail: n
		}
	}

	pub fn merge(r1: &Record, r2: &Record) -> Record {
		// Check output size
		let s1 = r1.get_num_columns();
		let s2 = r2.get_num_columns();
		let sn = s1 + s2;
		if sn > M {
			panic!("Merge results in record with {:?} columns, but max support is {:?}", sn, M);
		}

		let mut new_fields: [i32; M] = [0; M];

		// Add first record
		for i in 0..s1 {
			new_fields[i] = r1.fields[i];
		}

		// Add second record
		for i in s1..sn {
			new_fields[i] = r2.fields[i - s1];
		}

		Record {
			fields: new_fields,
			tail: sn
		}
	}

	pub fn get_column(&self, i: usize) -> &i32 {
		// Capacity OOB
		if i >= M {
			panic!("OOB Capacity");
		}
		// Index OOB
		if i >= self.tail {
			panic!("OOB Index")
		}

		&self.fields[i]
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

		// Move ownership of `value` to Record
		self.fields[i] = value;
	}

	pub fn get_column_values(&self) -> &[i32] {
		&self.fields[0..self.tail]
	}

	pub fn get_num_columns(&self) -> usize {
		self.tail
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