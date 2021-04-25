use joinlib::runner::run_all_joins;
use joinlib::runner::JoinRunResult;
use std::path::Path;
use std::fs::File;
use std::env;
use std::process;

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
		Err(e) => panic!("Could not read output file {:?}", e),
		Ok(f) => f
	};

	// Profile our joins on the input tables
	let results: Vec<JoinRunResult> = run_all_joins(table1_name, table2_name, 5, 5, 2, 2);

	// Write experiment run data to output file
	match serde_json::to_writer(outfile, &results) {
		Err(e) => panic!("Could not write to output file {:?}", e),
		Ok(_) => process::exit(0)
	}
}