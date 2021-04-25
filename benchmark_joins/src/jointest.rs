#[cfg(test)]
mod tests {
  use crate::join::*;
  use crate::record::Record;
  use crate::table::SimpleTable;
  const SMALL1: &str = "tables/small1.csv";
  const SMALL2: &str = "tables/small2.csv";
  const MED1: &str = "tables/med1.csv";
  const MED2: &str = "tables/med2.csv";

  fn nl_result(file1: &str, file2: &str, col1: usize, col2: usize) -> Vec<Record> {
    let table1 = &mut SimpleTable::new(file1);
    let table2 = &mut SimpleTable::new(file2);
    let mut nl = NestedLoopsJoin::new(table1, table2);
    nl.equi_join(col1, col2)
  }

  fn bnl_result(file1: &str, 
                file2: &str, 
                col1: usize, 
                col2: usize,
                l_block_sz: usize,
                r_block_sz: usize) -> Vec<Record> {
    let table1 = &mut SimpleTable::new(file1);
    let table2 = &mut SimpleTable::new(file2);
    let mut bnl = BlockNL::new(table1, table2, l_block_sz, r_block_sz);
    bnl.equi_join(col1, col2)
  }

  fn simplehash_result(file1: &str, file2: &str, col1: usize, col2: usize) -> Vec<Record> {
    let table1 = &mut SimpleTable::new(file1);
    let table2 = &mut SimpleTable::new(file2);
    let mut simplehash = SimpleHashJoin::new(table1, table2);
    simplehash.equi_join(col1, col2)
  }

  fn compare_results(actual: &mut Vec<Record>, expected: &mut Vec<Record>) {
    assert_eq!(actual.len(), expected.len());
    actual.sort();
    expected.sort();
    for i in 0..actual.len() {
      assert_eq!(actual[i], expected[i]);
    }
  }

  #[test]
  fn test_nl_small1_small2() {
    let res = nl_result(SMALL1, SMALL2, 2, 0);
    assert_eq!(res.len(), 3);
  }

  #[test]
  fn test_bnl_small1_small2() {
    let col1 = 2;
    let col2 = 0;
    let l_block_sz = 2;
    let r_block_sz = 2;
    let expected = nl_result(SMALL1, SMALL2, col1, col2);
    let mut actual = bnl_result(SMALL1, SMALL2, col1, col2, l_block_sz, r_block_sz);
    compare_results(&mut actual, &mut expected.clone());

    let l_block_sz = 6;
    let r_block_sz = 6;
    let mut actual = bnl_result(SMALL1, SMALL2, col1, col2, l_block_sz, r_block_sz);
    compare_results(&mut actual, &mut expected.clone());

    let l_block_sz = 1;
    let r_block_sz = 1;
    let mut actual = bnl_result(SMALL1, SMALL2, col1, col2, l_block_sz, r_block_sz);
    compare_results(&mut actual, &mut expected.clone());

    let l_block_sz = 5;
    let r_block_sz = 1;
    let mut actual = bnl_result(SMALL1, SMALL2, col1, col2, l_block_sz, r_block_sz);
    compare_results(&mut actual, &mut expected.clone());

    let l_block_sz = 3;
    let r_block_sz = 2;
    let mut actual = bnl_result(SMALL1, SMALL2, col1, col2, l_block_sz, r_block_sz);
    compare_results(&mut actual, &mut expected.clone());
  }

  #[test]
  fn test_bnl_med1_med2() {
    let col1 = 2;
    let col2 = 0;
    let l_block_sz = 10;
    let r_block_sz = 10;
    let expected = nl_result(MED1, MED2, col1, col2);
    let mut actual = bnl_result(MED1, MED2, col1, col2, l_block_sz, r_block_sz);
    compare_results(&mut actual, &mut expected.clone());

    let l_block_sz = 5;
    let r_block_sz = 5;
    let mut actual = bnl_result(MED1, MED2, col1, col2, l_block_sz, r_block_sz);
    compare_results(&mut actual, &mut expected.clone());

    let l_block_sz = 1;
    let r_block_sz = 1;
    let mut actual = bnl_result(MED1, MED2, col1, col2, l_block_sz, r_block_sz);
    compare_results(&mut actual, &mut expected.clone());

    let l_block_sz = 15;
    let r_block_sz = 2;
    let mut actual = bnl_result(MED1, MED2, col1, col2, l_block_sz, r_block_sz);
    compare_results(&mut actual, &mut expected.clone());

    let l_block_sz = 3;
    let r_block_sz = 2;
    let mut actual = bnl_result(MED1, MED2, col1, col2, l_block_sz, r_block_sz);
    compare_results(&mut actual,&mut expected.clone());
  }

  #[test]
  fn test_simplehash_small1_small2() { 
    let col1 = 2;
    let col2 = 0;
    let expected = nl_result(SMALL1, SMALL2, col1, col2);
    let mut actual = simplehash_result(SMALL1, SMALL2, col1, col2);
    compare_results(&mut actual, &mut expected.clone());
  }

  #[test]
  fn test_simplehash_med1_med2() { 
    let col1 = 2;
    let col2 = 0;
    let expected = nl_result(MED1, MED2, col1, col2);
    let mut actual = simplehash_result(MED1, MED2, col1, col2);
    compare_results(&mut actual, &mut expected.clone());
  }

}