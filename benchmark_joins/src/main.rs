mod readtable;
mod record;
mod table;
mod join;

use crate::join::NestedLoopsJoin;
use crate::join::BlockNL;

use crate::table::SimpleTable;


fn main() {
	let data = [1, 2, 3, 420];
	let rec = record::Record::new(&data);
	println!("{}", rec.get_column(3));
	println!("{}", rec.get_num_columns());

	let table1_name: &str = "tables/small1.csv";
	let table2_name: &str = "tables/small2.csv";

	let mut table1: SimpleTable = SimpleTable::new(table1_name);
	let mut table2: SimpleTable = SimpleTable::new(table2_name);

    println!("NL:");
	let mut join_algo: NestedLoopsJoin = NestedLoopsJoin::new(&mut table1, &mut table2);
	for r in join_algo.equi_join(2, 0).iter() {
		println!("Join record {:?}", r);
	}

    println!("blockNL:");
    let mut join_algo: BlockNL = BlockNL::new(&mut table1, &mut table2);
	for r in join_algo.equi_join(2, 0, 2, 2).iter() {
		println!("Join record {:?}", r);
	}
}