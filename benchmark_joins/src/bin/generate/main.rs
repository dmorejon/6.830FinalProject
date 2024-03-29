extern crate joinlib;
use std::fs;

use joinlib::table::SimpleTable;
use joinlib::record::Record;
pub mod tablegenerator;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use tablegenerator::generate_table;
use tablegenerator::generate_right_table;
use tablegenerator::write_table;


#[derive(Debug)]
struct LeftTableGenConfig {
	left_rows: usize,
	left_cols: usize,
	path: String,
}

#[derive(Debug)]
struct RightTableGenConfig {
	left_table: Vec<Record>,
	right_rows: usize,
	right_cols: usize,
	left_col: usize,
	right_col: usize,
	join_selectivity: f64,
	path: String,
}

fn format_row_count(mut num_rows: usize) -> String {
	// Figure out the right suffix for the row count
	// For example, thousands is K, millions is M
	let suffix: String;
	if num_rows <= 0 {
		panic!("What do you think you are doing");
	}
	else if num_rows < 1000 {
		suffix = "".to_owned();
	}
	else if num_rows < 1000 * 1000 {
		suffix = "K".to_owned();
		num_rows = num_rows / 1000;
	}
	else if num_rows < 1000 * 1000 * 1000 {
		suffix = "M".to_owned();
		num_rows = num_rows / (1000 * 1000);
	}
	else {
		panic!("Billion-or-more formatting is not supported");
	}

	// Add suffix to the number of rows
	let mut out = num_rows.to_string();
	out.push_str(&suffix);
	out
}

fn main() -> () {
	// General Params
	let join_name = "10K_left_select20";
	let tables_dir = format!("tables/{}/", join_name);
	let right_tables_dir = format!("tables/{}/rights", join_name);

	// Create dirs
	let _ = fs::create_dir_all(&tables_dir).unwrap();
	let _ = fs::create_dir_all(&right_tables_dir).unwrap();
	
	// Scaling factor
	// 1k for 10k left table
	// 10k for 100k left table
	// 100k for 1M left table
	// 1M for 10M left table, etc
	let scale = 1_000;

	// ******************** Left Table Params *******************
	let left_rows = 10 * scale;
	let left_cols = 10;
	println!("left_rows: {:?}", left_rows);

	let left_table_name = format!("{}R_{}C.csv", format_row_count(left_rows), left_cols);
	let left_path = tables_dir.to_owned() + &left_table_name;
	
	// **********************************************************
	
	let left_config = LeftTableGenConfig {
		left_rows: left_rows,
		left_cols: left_cols,
		path: left_path.clone(),
	};

	// To generate left table, uncomment following
	
	let left_table = generate_table(left_config.left_rows, left_config.left_cols);
	write_table(&left_table, &left_config.path);

	// To use existing left table, uncomment following
	let left_table = SimpleTable::new(&left_path).copy_to_vec_of_records();

	// ******************** Right Table Params ********************
	vec![2, 4, 6, 8, 10].par_iter().for_each(|i| {

		let right_rows = i * scale;
		let right_cols = 10;
		println!("right_rows: {:?}", right_rows);

		// Join Params
		let left_col = 5;
		let right_col = 5;
		let join_selectivity_perc = 20;

		let right_table_name = format!("{}R_{}C_select{}_left{}_right{}.csv", 
				format_row_count(right_rows), 
				format_row_count(right_cols), 
				join_selectivity_perc,
				left_col, right_col);

		// Ensure sizes + selectivity play friendly w/ each other
		assert!(right_rows >= ((join_selectivity_perc * left_rows) / 100));
		
		let right_path = tables_dir.to_owned() + "rights/" + &right_table_name;
	
		// **********************************************************

		let rc = RightTableGenConfig {
			left_table: left_table.clone(),
			right_rows: right_rows,
			right_cols: right_cols,
			left_col: left_col,
			right_col: right_col,
			join_selectivity: (join_selectivity_perc as f64) / 100.0,  
			path: right_path,
		};

		let right_table = generate_right_table(rc.left_table, rc.right_rows, rc.right_cols, rc.join_selectivity, rc.left_col, rc.right_col);
		write_table(&right_table, &rc.path);
	});
}
