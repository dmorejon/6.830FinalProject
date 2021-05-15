use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use crate::table::SimpleTable;
use crate::record::Record;
use rayon::iter::ParallelIterator;


pub struct ParallelNestedLoopsJoin<'a> {
  left: &'a mut SimpleTable,
  right: &'a mut SimpleTable,
}

impl<'a> ParallelNestedLoopsJoin<'a> {
  
  pub fn new(left: &'a mut SimpleTable, right: &'a mut SimpleTable) -> Self {
    Self {
      left,
      right
    }
  }

  pub fn equi_join(&mut self, left_col: usize, right_col: usize) -> Vec<Record> {
    // Number of records in left tables
    let left_size = self.left.get_num_records();

    self.left.record_par_iterator()
    .map(|left_record| {
      let mut intermediate_join_result = Vec::with_capacity(left_size);
      for right_record in self.right.record_iterator() {
        if left_record.get_column(left_col) == right_record.get_column(right_col) {
          // Join condition is met ==> new record 
          let join_record = Record::merge(left_record, right_record);
          intermediate_join_result.push(join_record);
        }
      }
      intermediate_join_result
    })
    .reduce(|| Vec::new(), |a, b| {
      let v = vec![a, b];
      itertools::concat(v)
    })
  }
}


pub struct ParallelSimpleHashJoin<'a> {
  left: &'a mut SimpleTable,
  right: &'a mut SimpleTable,
}

impl<'a> ParallelSimpleHashJoin<'a> {
  
  pub fn new(left: &'a mut SimpleTable, right: &'a mut SimpleTable) -> Self {
    Self {
      left,
      right
    }
  }

  pub fn equi_join(&mut self, left_col: usize, right_col: usize) -> Vec<Record> {
    // Number of records in left and right tables
    let left_size = self.left.get_num_records();
    let right_size = self.right.get_num_records();

    let mut hash_table: HashMap<&i32, Vec<&Record>> = HashMap::new();

    // Get the right table's view of its records
    let right_records = self.right.record_view();
    assert!(right_size == right_records.len());

    // Now we build the hash table on the smaller table
    // since this results in the fewest operations during join
    for r in right_records {
      let right_column_value = r.get_column(right_col);

      // Insert map from the hash of right join column value the record itself
      hash_table.entry(right_column_value).or_insert(Vec::new()).push(r);
    }

    let mut tmp = Vec::new();
    let join_results: Arc<Mutex<&mut Vec<Record>>> = Arc::new(Mutex::new(&mut tmp));

    self.left.record_par_iterator()
    .map(|left_record| {
      let mut intermediate_join_result = Vec::with_capacity(left_size);
      let left_column_value = left_record.get_column(left_col);
      match hash_table.get(left_column_value) {
        // If hash table doesn't have this value, 
        // we know for sure that this record does not 
        // participate in the join
        None => {},

        // But if there are some matches for the value,
        // then we know they ALL participate in the join
        Some(right_record_matches) => {
          for right_record in right_record_matches {
            let join_record: Record = Record::merge(left_record, right_record);
            intermediate_join_result.push(join_record);
          }
        }
      }
      intermediate_join_result
    })
    .for_each(|res| {
      let join_results_arc = Arc::clone(&join_results);
      let mut results = join_results_arc.lock().unwrap();
      (*results).extend(res);
    });
    
    tmp
  }
}