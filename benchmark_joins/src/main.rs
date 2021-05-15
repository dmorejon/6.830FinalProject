use joinlib::runner::run_one_join;
use joinlib::runner::JoinRunResult;
use joinlib::join::JoinAlgos;
use std::path::Path;
use std::fs::OpenOptions;
use std::io::BufReader;
use std::env;
use std::process;

fn main() {
	let args: Vec<String> = env::args().collect();
	if args.len() != 7+1 {
		println!("Expected [left_table] [right_tables] [json_outfile] [left_block_size] [right_block_size] [join_algo] [num_trials]");
		process::exit(1);
	}

	// Parse left table
	let left_table_name: &str = args.get(1).unwrap();

	// Parse right tables, separated by ;
	let mut right_table_names: Vec<String> = Vec::new();
	let raw_right_table_names: &str = args.get(2).unwrap();
	for rtn in raw_right_table_names.split(";") {
		right_table_names.push(rtn.to_owned());
	}

	// Parse left_block_size, right_block_size, join_algo, and num_trials
	let left_block_size: usize = args.get(4).unwrap().parse().unwrap();
	let right_block_size: usize = args.get(5).unwrap().parse().unwrap();
	let raw_join_algo: &str = args.get(6).unwrap();
	let num_trials: i8 = args.get(7).unwrap().parse().unwrap();

	// Match raw join algo to actual join algo
	let join_algo = match raw_join_algo {
		"nl" => JoinAlgos::NLJoin,
		"bnl" => JoinAlgos::BNLJoin,
		"hash" => JoinAlgos::SimpleHashJoin,
		"radix" => JoinAlgos::RadixJoin,
		_ => panic!("Unrecognized join algo {:?}", raw_join_algo),
	};

	// Fetch array of results from outfile
	let outpath = Path::new(args.get(3).unwrap());
	let mut results: Vec<JoinRunResult>;
	{
		let outfile = match OpenOptions::new()
			.create(true)
			.read(true)
			.write(true)
			.open(outpath) {
			Err(e) => panic!("Could not open {:?} {:?}", outpath, e),
			Ok(f) => f
		};
		results = match serde_json::from_reader(BufReader::new(&outfile)) {
			Err(e) => panic!("Could not read JSON {:?} {:?}", &outfile, e),
			Ok(r) => r
		};
	}

	// Profile our joins on the input tables

	println!("");
	println!("Left table 1 of 1...");

	for (i, rtn) in right_table_names.iter().enumerate() {
		println!("\tRight table {:?} of {:?}...", i+1, right_table_names.len());

		for trial in 1..=num_trials {
			println!("\t\tTrial {:?} of {:?}...", trial, num_trials);

			// Run the join and get its results
			let mut r = run_one_join(
				left_table_name, 
				&rtn, 
				5, 
				5, 
				left_block_size, 
				right_block_size,
				&join_algo
			);
			// Set the trial number
			r.trial_number = trial.clone();
			
			results.push(r);
		}
	}

	// Reopen outfile in write mode
	{
		let outfile = OpenOptions::new()
			.write(true)
			.open(outpath)
			.unwrap();
	
		// Write experiment run data to output file
		match serde_json::to_writer(outfile, &results) {
			Err(e) => panic!("Could not write to output file {:?}", e),
			Ok(_) => process::exit(0)
		};
	}

}