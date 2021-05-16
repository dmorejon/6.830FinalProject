use std::{collections::HashMap};
use crate::table::SimpleTable;
use crate::record::Record;
use itertools::Itertools;
use rayon::iter::{IndexedParallelIterator, IntoParallelRefMutIterator, ParallelIterator};

const CHUNK_SIZE: usize = 4_300;

pub struct ParallelNestedLoopsJoin<'a> {
	left: &'a mut SimpleTable,
	right: &'a mut SimpleTable,
}

impl<'a> ParallelNestedLoopsJoin<'a> {
	
	pub fn new(left: &'a mut SimpleTable, right: &'a mut SimpleTable) -> Self {
		Self {
			left,
			right
		}
	}

	pub fn equi_join(&mut self, left_col: usize, right_col: usize) -> Vec<Record> {
		// Number of records in left tables
		let left_size = self.left.get_num_records();

		self.left.record_par_iterator()
		.chunks(CHUNK_SIZE)
		.map(|left_records| {
			let mut intermediate_join_result = Vec::with_capacity(left_size);
			for lr in left_records {
				for rr in self.right.record_iterator() {
					if lr.get_column(left_col) == rr.get_column(right_col) {
						// Join condition is met ==> new record 
						let join_record = Record::merge(lr, rr);
						intermediate_join_result.push(join_record);
					}
				}
			}
			intermediate_join_result
		})
		.flatten()
		.collect()
	}
}


pub struct ParallelSimpleHashJoin<'a> {
	left: &'a mut SimpleTable,
	right: &'a mut SimpleTable,
}

impl<'a> ParallelSimpleHashJoin<'a> {
	
	pub fn new(left: &'a mut SimpleTable, right: &'a mut SimpleTable) -> Self {
		Self {
			left,
			right
		}
	}

	pub fn equi_join(&mut self, left_col: usize, right_col: usize) -> Vec<Record> {
		// Number of records in left and right tables
		let right_size = self.right.get_num_records();

		let mut hash_table: HashMap<&i32, Vec<&Record>> = HashMap::with_capacity(right_size);

		let right_records = self.right.record_view();
		assert!(right_size == right_records.len());

		// Now we build the hash table on the smaller table
		// since this results in the fewest operations during join
		for r in right_records {
			let right_column_value = r.get_column(right_col);

			// Map right join column value the record itself
			hash_table.entry(right_column_value).or_insert(Vec::new()).push(r);
		}
		
		self.left.record_par_iterator()
			.chunks(CHUNK_SIZE)
			// Map each left record chunk to group of joined records [R_1, ..., R_k]
			.map(|left_records| -> Vec<Record> {
				left_records.iter()
				.map(|lr| -> Vec<Record> {
					hash_table
						.get(lr.get_column(left_col)).unwrap_or(&Vec::new())
						.iter()
						.map(|rr| Record::merge(&lr, rr))
						.collect_vec()
				})
				.flatten()
				.collect()
			})
			// Flatten group of joined records into one [R_1, ..., R_k, ..., R_n]
			.flatten()
			
			// Collect as vec
			.collect()
	}
}

// Will mutate the tables by sorting in place
pub struct ParallelUnaryLeapFrogJoin<'a> {
	left: &'a mut SimpleTable,
	right: &'a mut SimpleTable,
}

impl<'a> ParallelUnaryLeapFrogJoin<'a> {
	
	pub fn new(left: &'a mut SimpleTable, right: &'a mut SimpleTable) -> Self {
		Self {
			left,
			right
		}
	}

	fn get_run_length(&self, table: &[Record], column: usize, mut i: usize) -> usize {
		// Determine the span, or "run length", of the table starting
		// at index i such that the for the next span-1 elements, the 
		// values of the table at the column are equivalent to that of 
		// table[i] at the column
		let mut run_length = 1;

		// Run throught the table starting at i, stopping if 
		// 1) we reach end of the table, OR
		// 2) we encounter a value not equal to table[i][column]
		while i < table.len() - 1 {
			i += 1;
			if table[i].get_column(column) != table[i - 1].get_column(column) {
				break;
			}
			run_length += 1;
		}
		
		run_length
	}

	pub fn equi_join(&mut self, left_col: usize, right_col: usize) -> Vec<Record> {
		// Number of records in left and right tables
		let left_size = self.left.get_num_records();
		let right_size = self.right.get_num_records();

		// Final tuples emitted by the join
		let mut join_results = Vec::with_capacity(left_size);

		// Sort the tables asynchronously and in parallel
		let mut tables = vec![
			(&mut self.left, left_col), 
			(&mut self.right, right_col)
		];
		tables.par_iter_mut().for_each(|tup| {
			tup.0.sort_by(tup.1);
		});

		let left_record_view = self.left.record_view();
		let right_record_view = self.right.record_view();

		// Run sort-merge fingering algorithm
		let mut l = 0;
		let mut r = 0;
		while l < left_size && r < right_size {

			// Run lengths
			let left_run_length = self.get_run_length(left_record_view, left_col, l);
			let right_run_length = self.get_run_length(right_record_view, right_col, r);
			
			// Join column values
			let left_value = left_record_view[l].get_column(left_col);
			let right_value = right_record_view[r].get_column(right_col);

			if left_value == right_value {
				// Collect the runs that match
				for ll in l..l+left_run_length {
					for rr in r..r+right_run_length {
						let join_record: Record = Record::merge(&left_record_view[ll], &right_record_view[rr]);
						join_results.push(join_record);
					}
				}
				// Move forward by the run length
				l += left_run_length;
				r += right_run_length;
			}
			else if left_value < right_value {
				l += left_run_length;
			}
			else {
				r += right_run_length;
			}
		}

		join_results
	}
}