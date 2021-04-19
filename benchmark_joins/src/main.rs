mod readtable;
mod record;
mod table;
mod join;

use crate::join::JoinAlg;

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

	// let mut join_algo: NestedLoopsJoin<i32> = NestedLoopsJoin::new(&mut table1, &mut table2);
// 	for r in join_algo.equi_join(2, 0).iter() {
// 		println!("Join record {:?}", r);
// 	}
    let result = join::run_join(JoinAlg::NestedLoops, &mut table1, &mut table2, 2, 0);
    for r in result.iter() {
        println!("Join record {:?}", r);
    }
}