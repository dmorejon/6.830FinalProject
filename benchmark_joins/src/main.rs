mod readtable;
mod record;
mod table;
mod join;

use strum::IntoEnumIterator;
use std::path::Path;
use std::fs::File;
use std::env;
use std::process;
use std::time::Duration;
use std::time::Instant;
use serde::{Deserialize, Serialize};

use crate::join::JoinAlgos;
use crate::join::NestedLoopsJoin;
use crate::join::BlockNL;
use crate::table::SimpleTable;

#[derive(Serialize, Deserialize, Debug)]
struct JoinRunResult {
	join_type: JoinAlgos,
	execution_time_nanos: u128,
	outer_table: String,
	inner_table: String,
}

fn flush_caches() -> () {
	// Allocate huge array of 1s
	// then write 0 everywhere
	const N: usize = 1000 * 1000;
	let mut a: [i32; N] = [1; N];
	for i in 0..N {
		a[i] = 0;
	}
}

fn run_bnl_join(bnlj: &mut BlockNL, left_col: usize, right_col: usize) -> Duration {
	flush_caches();
	let start: Instant = Instant::now();
	bnlj.equi_join(left_col, right_col);
	let end: Instant = Instant::now();
	end.duration_since(start)
}

fn run_nl_join(nlj: &mut NestedLoopsJoin, left_col: usize, right_col: usize) -> Duration {
	flush_caches();
	let start: Instant = Instant::now();
	nlj.equi_join(left_col, right_col);
	let end: Instant = Instant::now();
	end.duration_since(start)
}

fn do_join(table1_name: &str, table2_name: &str,
					 left_col: usize, right_col: usize,
					 algo: JoinAlgos, l_block_sz: usize, 
           r_block_sz: usize, ) -> JoinRunResult {
	// Create tables
	let table1: &mut SimpleTable = &mut SimpleTable::new(table1_name);
	let table2: &mut SimpleTable = &mut SimpleTable::new(table2_name);

	// Measure execution time
	let execution_time: Duration;
	match algo {
		JoinAlgos::NLJoin => {
			execution_time = run_nl_join(&mut NestedLoopsJoin::new(table1, table2), left_col, right_col)
		}
		JoinAlgos::BNLJoin => {
			execution_time = run_bnl_join(&mut BlockNL::new(table1, table2, l_block_sz, r_block_sz), left_col, right_col)
		},
	}

	// Output execution time
	JoinRunResult {
		join_type: algo,
		execution_time_nanos: execution_time.as_nanos(),
		outer_table: table1_name.to_owned(),
		inner_table: table2_name.to_owned(),
	}
}

fn main() {
	// Expect two tables and an output file
	let args: Vec<String> = env::args().collect();
	if args.len() != 4 {
		println!("{:?}", args);
		println!("Expected two tables [fname1] [fname2] [json_outfile]");
		process::exit(1);
	}

	// Parse two tables and file
	let table1_name: &str = args.get(1).unwrap();
	let table2_name: &str = args.get(2).unwrap();
	let outfile: File = match File::create(&Path::new(args.get(3).unwrap())) {
		Err(e) => panic!("Could not read output file"),
		Ok(f) => f
	};

	// Profile our joins on the input tables
	let mut results: Vec<JoinRunResult> = Vec::new();
	for algo in JoinAlgos::iter() {
		let result: JoinRunResult = do_join(table1_name, table2_name, 2, 0, algo.clone(), 2, 2);
		let result_flipped: JoinRunResult = do_join(table2_name, table1_name, 2, 0, algo.clone(), 2, 2);
		results.push(result);
		results.push(result_flipped);
	}

	// Write experiment run data to output file
	match serde_json::to_writer(outfile, &results) {
		Err(e) => panic!("Could not write to output file {:?}", e),
		Ok(_) => process::exit(0)
	}
}