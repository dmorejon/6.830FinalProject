
use crate::record::Record;
use std::{time::Instant};
use strum::IntoEnumIterator;
use serde::{Deserialize, Serialize};

use crate::join::BlockNL;
use crate::join::NestedLoopsJoin;
use crate::join::SimpleHashJoin;
use crate::join::JoinAlgos;

use crate::radixjoin::RadixJoin;
use crate::parjoin::*;
use crate::table::SimpleTable;

#[derive(Serialize, Deserialize, Debug)]
pub struct Table {
	table_name: String,
	num_records: usize,
	columns_per_record: usize,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JoinAlgoDetails {
	join_name: JoinAlgos,
	left_block_size: usize,
	right_block_size: usize,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JoinRunResult {
	join_type: JoinAlgoDetails,
	execution_time_nanos: u128,
	outer_table: Table,
	inner_table: Table,
	num_emitted_records: usize,
	pub trial_number: i8
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
		join_type: JoinAlgoDetails {
			join_name: JoinAlgos::BNLJoin,
			left_block_size: bnlj.get_left_block_size(),
			right_block_size: bnlj.get_right_block_size(),
		},
		execution_time_nanos: end.duration_since(start).as_nanos(),
		outer_table: t1,
		inner_table: t2,
		num_emitted_records: results.len(),
		trial_number: -1,
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
		join_type: JoinAlgoDetails {
			join_name: JoinAlgos::NLJoin,
			left_block_size: 0,
			right_block_size: 0,
		},
		execution_time_nanos: end.duration_since(start).as_nanos(),
		outer_table: t1,
		inner_table: t2,
		num_emitted_records: results.len(),
		trial_number: -1,
	}
}

fn run_pnl_join(pnlj: &mut ParallelNestedLoopsJoin, left_col: usize, right_col: usize, t1: Table, t2: Table) -> JoinRunResult {
	// Run the join
	flush_caches();
	let start: Instant = Instant::now();
	let results: Vec<Record> = pnlj.equi_join(left_col, right_col);
	let end: Instant = Instant::now();

	// Output result
	JoinRunResult {
		join_type: JoinAlgoDetails {
			join_name: JoinAlgos::PNLJoin,
			left_block_size: 0,
			right_block_size: 0,
		},
		execution_time_nanos: end.duration_since(start).as_nanos(),
		outer_table: t1,
		inner_table: t2,
		num_emitted_records: results.len(),
		trial_number: -1,
	}
}

fn run_simplehash_join(shj: &mut SimpleHashJoin, left_col: usize, right_col: usize, t1: Table, t2: Table) -> JoinRunResult {
	// Run the join
	flush_caches();
	let start: Instant = Instant::now();
	let results: Vec<Record> = shj.equi_join(left_col, right_col);
	let end: Instant = Instant::now();

	// Output result
	JoinRunResult {
		join_type: JoinAlgoDetails {
			join_name: JoinAlgos::SimpleHashJoin,
			left_block_size: 0,
			right_block_size: 0,
		},
		execution_time_nanos: end.duration_since(start).as_nanos(),
		outer_table: t1,
		inner_table: t2,
		num_emitted_records: results.len(),
		trial_number: -1,
	}
}

fn run_radix_join(rj: &mut RadixJoin, left_col: usize, right_col: usize, t1: Table, t2: Table) -> JoinRunResult {
	// Run the join
	flush_caches();
	let start: Instant = Instant::now();
	let results: Vec<Record> = rj.equi_join(left_col, right_col);
	let end: Instant = Instant::now();

	// Output result
	JoinRunResult {
		join_type: JoinAlgoDetails {
			join_name: JoinAlgos::RadixJoin,
			left_block_size: 0,
			right_block_size: 0,
		},
		execution_time_nanos: end.duration_since(start).as_nanos(),
		outer_table: t1,
		inner_table: t2,
		num_emitted_records: results.len(),
		trial_number: -1,
	}
}

fn run_psh_join(pshj: &mut ParallelSimpleHashJoin, left_col: usize, right_col: usize, t1: Table, t2: Table) -> JoinRunResult {
	// Run the join
	flush_caches();
	let start: Instant = Instant::now();
	let results: Vec<Record> = pshj.equi_join(left_col, right_col);
	let end: Instant = Instant::now();

	// Output result
	JoinRunResult {
		join_type: JoinAlgoDetails {
			join_name: JoinAlgos::ParallelSimpleHashJoin,
			left_block_size: 0,
			right_block_size: 0,
		},
		execution_time_nanos: end.duration_since(start).as_nanos(),
		outer_table: t1,
		inner_table: t2,
		num_emitted_records: results.len(),
		trial_number: -1,
	}
}

fn run_pulf_join(pulf: &mut ParallelUnaryLeapFrogJoin, left_col: usize, right_col: usize, t1: Table, t2: Table) -> JoinRunResult {
	// Run the join
	flush_caches();
	let start: Instant = Instant::now();
	let results: Vec<Record> = pulf.equi_join(left_col, right_col);
	let end: Instant = Instant::now();

	// Output result
	JoinRunResult {
		join_type: JoinAlgoDetails {
			join_name: JoinAlgos::ParallelUnaryLeapFrogJoin,
			left_block_size: 0,
			right_block_size: 0,
		},
		execution_time_nanos: end.duration_since(start).as_nanos(),
		outer_table: t1,
		inner_table: t2,
		num_emitted_records: results.len(),
		trial_number: -1,
	}
}

pub fn run_one_join(
	table1_name: &str, 
	table2_name: &str,
	left_col: usize,
	right_col: usize,
	l_block_sz: usize, 
	r_block_sz: usize,
	algo: &JoinAlgos) -> JoinRunResult {
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
				t1, t2),
		JoinAlgos::SimpleHashJoin =>
			run_simplehash_join(
				&mut SimpleHashJoin::new(table1, table2), 
				left_col, 
				right_col,
				t1, t2),
		JoinAlgos::RadixJoin =>
			run_radix_join(
				&mut RadixJoin::new(table1, table2), 
				left_col, 
				right_col,
				t1, t2),
		JoinAlgos::PNLJoin => {
			run_pnl_join(
				&mut ParallelNestedLoopsJoin::new(table1, table2), 
				left_col, 
				right_col,
				t1, t2)
			},
		JoinAlgos::ParallelSimpleHashJoin => {
			run_psh_join(
				&mut ParallelSimpleHashJoin::new(table1, table2), 
				left_col, 
				right_col,
				t1, t2)
		},
		JoinAlgos::ParallelUnaryLeapFrogJoin => {
			run_pulf_join(
				&mut ParallelUnaryLeapFrogJoin::new(table1, table2), 
				left_col, 
				right_col,
				t1, t2)
		},
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
		println!("Running join {:?} on {:?} as left and {:?} as right...", algo, table1_name, table2_name);
		let result: JoinRunResult = run_one_join(
			table1_name, table2_name, 
			left_col, right_col, 
			l_block_sz, r_block_sz, &algo);
		println!("Finished join! Took {:?} millis", result.execution_time_nanos / 1e6 as u128);
		results.push(result);
	}
	results
}