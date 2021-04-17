
use std::process;
mod readtable;

fn main() {
    println!("Hello, world!");
    if let Err(err) = readtable::example("src/dummy.csv") {
        println!("error running example: {}", err);
        process::exit(1);
    }
}
