use core::str::FromStr;
use core::fmt::Debug;
use crate::record::Record;
use crate::SimpleTable;

pub trait Join<'a, T> {
  fn new(left: &'a mut SimpleTable<T>, right: &'a mut SimpleTable<T>) ->Self;
  fn equi_join(&mut self, left_col: usize, right_col: usize) -> Vec<Record<T>>;
}

pub enum JoinAlg {
  NestedLoops,
  // BlockNL,
}

pub struct NestedLoopsJoin<'a, T> {
  left: &'a mut SimpleTable<T>,
  right: &'a mut SimpleTable<T>,
}

pub fn run_join<'a, T> (
      alg: JoinAlg, 
      left: &'a mut SimpleTable<T>, 
      right: &'a mut SimpleTable<T>, 
      left_col: usize, 
      right_col: usize) 
      -> Vec<Record<T>>
      where T: Clone + Debug + FromStr + PartialEq, <T as FromStr>::Err: Debug {
  let mut join = match alg {
    JoinAlg::NestedLoops => NestedLoopsJoin::new(left, right),
    // JoinAlg::BlockNL => BlockNL::new(left, right),
    // _ => panic!("alg not implemeted"),
  };
  join.equi_join(left_col, right_col)
}

impl<'a, T> Join<'a, T> for NestedLoopsJoin<'a, T> where T: Clone + Debug + FromStr + PartialEq, <T as FromStr>::Err: Debug {
  
  fn new(left: &'a mut SimpleTable<T>, right: &'a mut SimpleTable<T>) -> Self {
    Self {
      left,
      right
    }
  }

  fn equi_join(&mut self, left_col: usize, right_col: usize) -> Vec<Record<T>> {
    // TODO: Remove the duplicated concatenation
    let mut join_result: Vec<Record<T>> = Vec::new();

    let left_num_records: usize = self.left.get_num_records();
    let right_num_records: usize = self.right.get_num_records();

    for _l in 0..left_num_records {
      let mut left_record: Record<T> = self.left.read_next_record().clone();

      for _r in 0..right_num_records {
        let mut right_record: Record<T> = self.right.read_next_record();

        if left_record.get_column(left_col) == right_record.get_column(right_col) {
          // Join condition is met ==> new record 
          let join_record: Record<T> = Record::merge(&mut left_record, &mut right_record);
          join_result.push(join_record);
        }
      }
      self.right.rewind();
    }

    join_result
  }
}