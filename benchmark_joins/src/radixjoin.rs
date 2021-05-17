use std::collections::HashMap;

use rayon::iter::{IntoParallelIterator, IntoParallelRefMutIterator, ParallelIterator};

use crate::{record::Record};
use crate::table::SimpleTable;

pub struct RadixJoin<'a> {
	left: &'a mut SimpleTable,
	right: &'a mut SimpleTable,
}

// Leftmost bits
fn h1_1(x: i32) -> i32 {
	x & 0b11_111
}

// Next leftmost bits
fn h1_2(x: i32) -> i32 {
	(x & 0b1_111_100_000) >> 5
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
		
		(0..left_partitions.len()).into_par_iter()
		.map(|first| -> Vec<Record> {
			(0..left_partitions[first].len()).into_par_iter()
			.map(|second| -> Vec<Record> {
				// Build hash table on right partition corresponding to [first][second]
				let right_partition = &right_partitions[first][second];
				let mut right_table = HashMap::<i32, Vec<&Record>>::new();
				for record in right_partition {
					let right_column_value = *record.get_column(right_col);
					right_table.entry(right_column_value).or_insert(Vec::new()).push(record);
				}
				// Probe built hash table
				left_partitions[first][second].iter()
				.map(|lr| -> Vec<Record> {
					right_table
							.get(lr.get_column(left_col)).unwrap_or(&Vec::new())
							.iter()
							.map(|rr| Record::merge(&lr, rr))
							.collect()
				})
				.flatten()
				.collect()
			})
			.flatten()
			.collect()
		})
		.flatten()
		.collect()
	}	
}