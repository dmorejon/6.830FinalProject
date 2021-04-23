extern crate joinlib;
use joinlib::record::Record;
use tablegenerator::generate_table;
mod tablegenerator;

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
	let join_dir = "tables/join1/";
	let left_table_name = "1MR_10C.csv";
	
	let left_config = LeftTableGenConfig {
		left_rows: 1000 * 1000,
		left_cols: 10,
		path: join_dir.to_owned() + left_table_name,
	};

	let left_table = generate_table(left_config.left_rows, left_config.left_cols);
	

	let right_table_name = "1MR_10C_select40_left5_right5.csv";
	let right_config = RightTableGenConfig {
		left_table,
		right_rows: 1000 * 1000,
		right_cols: 10,
		left_col: 5,
		right_col: 5,
		join_selectivity: 0.4,  
		path: join_dir.to_owned() + "/rights/" + right_table_name,
	};

	println!("left config: {:?}", left_config);

	println!("right config: {:?}", right_config);
	println!("I am rusty");
}