mod readtable;
mod record;
mod table;

use std::process;

use crate::table::SimpleTable;

fn main() {
	let file_name: &str = "tables/dummy.csv";

	match readtable::fetch_records::<i32>(file_name) {
		Ok(table) => {
			println!("{:?}", table);
		},
		Err(e) => {
			println!("error running example: {}", e);
			process::exit(1);
		}
	}

	let data = [1, 2, 3, 420];
	let rec = record::Record::new(&data);
	println!("{}", rec.get_column(3));
	println!("{}", rec.get_num_columns());

	let mut table: SimpleTable<i32> = SimpleTable::new(file_name);
	println!("Number of records {:?}", table.get_num_records());
	for i in 1..=table.get_num_records() {
		println!("Record {:?} : {:?}", i, table.read_next_record());
	}
}