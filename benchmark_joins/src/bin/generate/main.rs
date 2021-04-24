extern crate joinlib;
use joinlib::record::Record;
pub mod tablegenerator;
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

fn main() -> () {
	// IMPORTANT:
	// YOU MUST MAKE SURE THAT THE DIRECTORIES EXIST.
	// 	- tables/joinN
	// 	- tables/joinN/rights/
	let join_dir = "tables/join1/";
	let left_table_name = "1MR_10C.csv";
	
	let left_config = LeftTableGenConfig {
		left_rows: 1000 * 1000,
		left_cols: 10,
		path: join_dir.to_owned() + left_table_name,
	};

	let left_table = generate_table(left_config.left_rows, left_config.left_cols);
	write_table(&left_table, &left_config.path);


	let right_table_name = "1MR_10C_select40_left5_right5.csv";
	let rc = RightTableGenConfig {
		left_table,
		right_rows: 1000 * 1000,
		right_cols: 10,
		left_col: 5,
		right_col: 5,
		join_selectivity: 0.4,  
		path: join_dir.to_owned() + "rights/" + right_table_name,
	};

	let right_table = generate_right_table(rc.left_table, rc.right_rows, rc.right_cols, rc.join_selectivity, rc.left_col, rc.right_col);
	write_table(&right_table, &rc.path);

	println!("I am rusty");
}