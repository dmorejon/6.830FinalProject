
use std::time::Instant;
use core::time::Duration;
use strum::IntoEnumIterator;
use serde::{Deserialize, Serialize};

use crate::BlockNL;
use crate::NestedLoopsJoin;
use crate::SimpleTable;
use crate::JoinAlgos;

#[derive(Serialize, Deserialize, Debug)]
pub struct JoinRunResult {
	join_type: JoinAlgos,
	execution_time_nanos: u128,
	outer_table: String,
	inner_table: String,
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
  let table2: &mut SimpleTable = &mut SimpleTable::new(table2_name);

  // Measure execution time
  let execution_time: Duration;
  match algo {
    JoinAlgos::NLJoin => {
      execution_time = run_nl_join(
        &mut NestedLoopsJoin::new(table1, table2), 
        left_col, 
        right_col)
    }
    JoinAlgos::BNLJoin => {
      execution_time = run_bnl_join(
        &mut BlockNL::new(table1, table2, l_block_sz, r_block_sz), 
        left_col, 
        right_col)
    },
  }

  // Output execution time
  JoinRunResult {
    join_type: algo.clone(),
    execution_time_nanos: execution_time.as_nanos(),
    outer_table: table1_name.to_owned(),
    inner_table: table2_name.to_owned(),
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