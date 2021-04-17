use std::process;
mod readtable;
mod record;
mod table;

fn main() {
    println!("Hello, world!");
    if let Err(err) = readtable::example("tables/dummy.csv") {
        println!("error running example: {}", err);
        process::exit(1);
    }

    let data = [1, 2, 3, 420];
    let rec = record::Record{data: data};
    println!("{}", rec.get_column(3));
    println!("{}", rec.get_num_columns());

    // let table = table::SimpleTable{[rec], 0};
}