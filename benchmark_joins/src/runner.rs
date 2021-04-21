
use crate::record::Record;
use std::time::Instant;
use strum::IntoEnumIterator;
use serde::{Deserialize, Serialize};

use crate::BlockNL;
use crate::NestedLoopsJoin;
use crate::SimpleTable;
use crate::JoinAlgos;

#[derive(Serialize, Deserialize, Debug)]
pub struct Table {
	table_name: String,
	num_records: usize,
	columns_per_record: usize,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JoinRunResult {
	join_type: JoinAlgos,
	execution_time_nanos: u128,
	outer_table: Table,
	inner_table: Table,
	num_emitted_records: usize,
}

fn flush_caches() -> () {
	// TODO confirm that this actually
	// flushes the caches!
	//
	// Allocate huge array of 1s
	// then write 0 everywhere
	const N: usize = 1000 * 1000;
	let mut a: [i32; N] = [1; N];
	for i in 0..N {
		a[i] = 0;
	}
}

fn run_bnl_join(bnlj: &mut BlockNL, left_col: usize, right_col: usize, t1: Table, t2: Table) -> JoinRunResult {
	// Run the join
	flush_caches();
	let start: Instant = Instant::now();
	let results: Vec<Record> = bnlj.equi_join(left_col, right_col);
	let end: Instant = Instant::now();

	// Output result
	JoinRunResult {
		join_type: JoinAlgos::BNLJoin,
		execution_time_nanos: end.duration_since(start).as_nanos(),
		outer_table: t1,
		inner_table: t2,
		num_emitted_records: results.len(),
	}
}

fn run_nl_join(nlj: &mut NestedLoopsJoin, left_col: usize, right_col: usize, t1: Table, t2: Table) -> JoinRunResult {
	// Run the join
	flush_caches();
	let start: Instant = Instant::now();
	let results: Vec<Record> = nlj.equi_join(left_col, right_col);
	let end: Instant = Instant::now();

	// Output result
	JoinRunResult {
		join_type: JoinAlgos::NLJoin,
		execution_time_nanos: end.duration_since(start).as_nanos(),
		outer_table: t1,
		inner_table: t2,
		num_emitted_records: results.len(),
	}
}

fn run_one_join(
	table1_name: &str, 
	table2_name: &str,
	left_col: usize,
	right_col: usize,
	l_block_sz: usize, 
	r_block_sz: usize,
	algo: JoinAlgos) -> JoinRunResult {
	// Create tables
	let table1: &mut SimpleTable = &mut SimpleTable::new(table1_name);
	let t1: Table = Table {
		table_name: table1_name.to_owned(),
		num_records: table1.get_num_records(),
		columns_per_record: table1.get_num_columns_per_record(),
	};
	let table2: &mut SimpleTable = &mut SimpleTable::new(table2_name);
	let t2: Table = Table {
		table_name: table2_name.to_owned(),
		num_records: table2.get_num_records(),
		columns_per_record: table2.get_num_columns_per_record(),
	};

	// Dispatch experiment and result measurement
	match algo {
		JoinAlgos::NLJoin => 
			run_nl_join(
				&mut NestedLoopsJoin::new(table1, table2), 
				left_col, 
				right_col,
				t1, t2),
		JoinAlgos::BNLJoin => 
			run_bnl_join(
				&mut BlockNL::new(table1, table2, l_block_sz, r_block_sz), 
				left_col, 
				right_col,
				t1, t2)
	}
}

pub fn run_all_joins(
	table1_name: &str, 
	table2_name: &str,
	left_col: usize,
	right_col: usize,
	l_block_sz: usize, 
	r_block_sz: usize) -> Vec<JoinRunResult> {
		
	// Profile our joins on the input tables
	let mut results: Vec<JoinRunResult> = Vec::new();
	for algo in JoinAlgos::iter() {
		// Run join as table1 J table2
		let result: JoinRunResult = run_one_join(
			table1_name, table2_name, 
			left_col, right_col, 
			l_block_sz, r_block_sz, algo.clone());
		results.push(result);

		// Run join as table2 J table1
		let result_reversed: JoinRunResult = run_one_join(
			table2_name, table1_name, 
			right_col, left_col, 
			r_block_sz, l_block_sz, algo.clone());
		results.push(result_reversed);
	}
	results
}