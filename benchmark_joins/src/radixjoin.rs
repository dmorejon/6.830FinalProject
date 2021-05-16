use std::collections::HashMap;

use rayon::iter::{IntoParallelIterator, IntoParallelRefMutIterator, ParallelIterator};

use crate::{join, record::Record};
use crate::table::SimpleTable;

pub struct RadixJoin<'a> {
	left: &'a mut SimpleTable,
	right: &'a mut SimpleTable,
}


	// Leftmost 4 bits
	fn h1_1(x: i32) -> i32 {x & 0b11111}
	// Rightmost 4 bits

	fn h1_2(x: i32) -> i32 {
		x.abs() >> 26
	}

fn partition(table: &mut SimpleTable, 
									 col: usize, 
									 num_first: i32, 
									 num_second: i32) -> Vec<Vec<Vec<Record>>> {

	let size = table.get_num_records();

	// Don't know the rustian way to do this lol
	let mut first_partitions: Vec<Vec<Record>> = Vec::with_capacity(num_first as usize);
	for _i in 0..num_first {
		first_partitions.push(Vec::with_capacity(size / (num_first as usize)));
	}

	// Make first partitions
	for _i in 0..size {
		let record = table.read_next_record();
		let part = h1_1(*record.get_column(col)) as usize;
		first_partitions[part].push(record.clone());
	}
	table.rewind();


	// Use first partitions to make second partitions

	// Don't know the rustian way to do this lol
	let mut result: Vec<Vec<Vec<Record>>> = Vec::with_capacity(num_first as usize);

	// Result[0] is vector of second partitions that are in h1_1(x) = 0
	// Result[0][0] is the records that are in h1_1(x) = 0 and h1_2(x) = 0

	for partition in first_partitions {
		let mut second_partitions: Vec<Vec<Record>> = Vec::with_capacity(num_first as usize);
		for _i in 0..num_second {
			second_partitions.push(Vec::with_capacity(size / ((num_first * num_second) as usize)));
		}

		for record in partition {
			let val = *record.get_column(col);
			let part = h1_2(val) as usize;
			second_partitions[part].push(record.clone());
		}
		result.push(second_partitions);
	}

	// *****************************
	// let mut min = usize::MAX;
	// let mut max = usize::MIN;
	// let mut avg = 0;
	// let mut num = 0;
	// for first in &result {
	//   for second in first {
	//     if second.len() < min {
	//       min = second.len();
	//     }
	//     if second.len() > max {
	//       max = second.len();
	//     }
	//     avg += second.len();
	//   }
	//   num += first.len();
	// }
	// avg = avg / num;
	// println!("min: {:?}, max: {:?}, avg: {:?}", min, max, avg);
// *****************************
	result
}


impl<'a> RadixJoin<'a> {
	
	pub fn new(left: &'a mut SimpleTable, right: &'a mut SimpleTable) -> Self {
		Self {
			left,
			right
		}
	}

	pub fn equi_join(&mut self, left_col: usize, right_col: usize) -> Vec<Record> {
		// TODO: potentially use a tuneable variable like these 
		//  and define h1_1, h1_2 based on that
		let first_bits = 5;
		let second_bits = 5;

		let base: i32 = 2;
		let num_first_partition= base.pow(first_bits);
		let num_second_partition = base.pow(second_bits);

		let mut tables = vec![
			(&mut self.left, left_col), 
			(&mut self.right, right_col)
		];

		let partitions: Vec<Vec<Vec<Vec<Record>>>> = tables
			.par_iter_mut()
			.map(|tup| {
				partition(tup.0,  tup.1, num_first_partition, num_second_partition)
			})
			.collect();

		let left_partitions = &partitions[0];
		let right_partitions = &partitions[1];

		let left_size = self.left.get_num_records();
		let mut join_result = Vec::with_capacity(left_size);

		for first in 0..left_partitions.len() {
			let join_first_result: Vec<Record> = (0..left_partitions[first].len()).into_par_iter()
			.map(|second| -> Vec<Record> {
				// Build hash table on right partition corresponding to [first][second]
				let right_partition = &right_partitions[first][second];
				let mut right_table = HashMap::<i32, Vec<&Record>>::new();
				for record in right_partition {
					let right_column_value = *record.get_column(right_col);
					// TODO should keys be a Record or &Record
					right_table.entry(right_column_value).or_insert(Vec::new()).push(record);
				}
				// Probe built hash table
				let mut intermediate_results = Vec::new();
				for left_record in &left_partitions[first][second] {
					let left_column_value = left_record.get_column(left_col);
					match right_table.get(left_column_value) {
						// If hash table doesn't have this value, 
						// we know for sure that this record does not 
						// participate in the join
						None => continue,
		
						// But if there are some matches for the value,
						// then we know they ALL participate in the join
						Some(right_record_matches) => {
							for right_record in right_record_matches {
								let join_record: Record = Record::merge(left_record, right_record);
								intermediate_results.push(join_record);
							}
						}
					};
				}
				intermediate_results
			})
			.flatten()
			.collect();

			join_result.extend(join_first_result);
		}
		join_result
	}
}