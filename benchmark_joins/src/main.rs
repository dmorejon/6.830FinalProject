use joinlib::runner::run_all_joins;
use joinlib::runner::JoinRunResult;
use std::path::Path;
use std::fs::File;
use std::env;
use std::process;

fn main() {
	// Expect two table(s) and an output file
	let args: Vec<String> = env::args().collect();
	if args.len() != 4 {
		println!("{:?}", args);
		println!("Expected two tables [left_table] [right_tables] [json_outfile]");
		process::exit(1);
	}

	// Parse left table and out file
	let left_table_name: &str = args.get(1).unwrap();
	let outfile: File = match File::create(&Path::new(args.get(3).unwrap())) {
		Err(e) => panic!("Could not read output file {:?}", e),
		Ok(f) => f
	};

	// Parse right tables, separated by ;
	let mut right_table_names: Vec<String> = Vec::new();
	let raw_right_table_names: &str = args.get(2).unwrap();
	for rtn in raw_right_table_names.split(";") {
		right_table_names.push(rtn.to_owned());
	}
	println!("Right tables: {:?}", right_table_names);

	// Profile our joins on the input tables
	let mut results: Vec<Vec<JoinRunResult>> = Vec::new();
	for rtn in right_table_names {
		results.push(run_all_joins(
			left_table_name, 
			&rtn, 
			5, 
			5, 
			2, 
			2));
	}

	// Write experiment run data to output file
	match serde_json::to_writer(outfile, &results) {
		Err(e) => panic!("Could not write to output file {:?}", e),
		Ok(_) => process::exit(0)
	}
}