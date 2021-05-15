extern crate joinlib;
use joinlib::record::Record;
use joinlib::record::M;

use std::panic;


#[cfg(test)]
mod tests {
	use super::*;
	use rand::{seq::SliceRandom, thread_rng};

	fn check_size(r: &Record, expected_size: usize) {
		assert_eq!(expected_size, r.get_num_columns());
	}

	fn check_column(r: &Record, col_idx: usize, expected_value: &i32) {
		assert_eq!(expected_value, r.get_column(col_idx));
	}

	fn check_columns_in_order(r: &Record, columns: &[i32]) {
		for i in 0..columns.len() {
			check_column(r, i, &columns[i]);
		}
	}
	
	fn check_columns_randomly(r: &Record, columns: &[i32]) {
		// Populate indexs
		let mut indexes: Vec<usize> = (0..columns.len()).collect();

		// Shuffle indexes
		indexes.shuffle(&mut thread_rng());

		// Check column values
		for i in indexes {
			check_column(r, i, &columns[i])
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

	// #[test]
	// fn test_large_record() {
	// 	// Thiqq
	// 	let mut columns: [i32; 75] = [0; 75];
	// 	for v in 1000..1075 {
	// 		columns[v - 1000] = v as i32;
	// 	}
	// 	let r = Record::new(&columns);
	// 	check_record(&r, &columns);
	// }

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