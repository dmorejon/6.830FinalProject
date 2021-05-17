# 6.830FinalProject

This repository contains various in-memory equi-join algorithms and associated profiling code. There is also support for generating tables with specific properties like number of rows, columns, and output join selectivity. 

## Benchmark Running Instructions

To run benchmarking, navigate to `6.830FinalProject/benchmark_joins`. From there, you can run the `profiler.sh` script via, e.g.,:

```./profiler.sh tables/10K_left_select10 hash,bnl,nl 3```

The possible algorithm names are `nl, bnl, pnl, hash, psh, radix, pulf`. The command will run the different joins specified for 3 trials each, time them, and put the results in `outfile_json`. Specifically, this will **overwrite the contents of file**. For example, the command above will generate `experiments/10K_left_select10/10K_left_select10.json` and have content similar to:
```JSON
[
  {
    "join_type":{
      "join_name":"SimpleHashJoin",
      "left_block_size":0,
      "right_block_size":0
    },
    "execution_time_nanos":1727638,
    "outer_table":{
      "table_name":"tables/10K_left_select10/10KR_10C.csv",
      "num_records":10000,
      "columns_per_record":10
    },
    "inner_table":{
      "table_name":"tables/10K_left_select10/rights/10KR_10C_select10_left5_right5.csv",
      "num_records":10000,
      "columns_per_record":10
    },
    "num_emitted_records":1000,
    "trial_number":1
  },
  {
    "join_type":{
      "join_name":"SimpleHashJoin",
      "left_block_size":0,
      "right_block_size":0
    },
    "execution_time_nanos":1265206,
    "outer_table":{
      "table_name":"tables/10K_left_select10/10KR_10C.csv",
      "num_records":10000,
      "columns_per_record":10
    },
    "inner_table":{
      "table_name":"tables/10K_left_select10/rights/10KR_10C_select10_left5_right5.csv",
      "num_records":10000,
      "columns_per_record":10
    },
    "num_emitted_records":1000,
    "trial_number":2
  },
  ...
  ...
]
```

Note the Block-Nested-Loops Join will be run with a combination of different block sizes that is specified in the profiling script. Further, the profiling script calls `cargo run` under the hood. Specifically, this is also a valid run

```cargo run --release [left_table] [right_tables] [json_outfile] [left_block_size] [right_block_size] [join_algo] [num_trials]```

## Table Generation Instructions
   To generate a table, go to `src/bin/generate/main.rs` and change the config structs, then run with
   ```cargo run --release --bin generate```
