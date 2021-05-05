# 6.830FinalProject

## Benchmark Running Instructions

To run benchmarking, navigate to `6.830FinalProject/benchmark_joins`. From there, you can run the `profiler.sh` script via, e.g.,:

```./profiler.sh tables/10K_left_select10 hash,nl,bnl```

And that will run the different joins specified, time them, and put the results in `outfile_json`. Specifically, this will **overrwite the contents of file**. For example, the command above will generate `experiments/10K_left_select10/10K_left_select10.json` and have content similar to:
```JSON
[
  {
    "join_type": {
      "join_name": "SimpleHashJoin",
      "left_block_size": 0,
      "right_block_size": 0
    },
    "execution_time_nanos": 10442650,
    "outer_table": {
      "table_name": "tables/10K_left_select10/10KR_10C.csv",
      "num_records": 10000,
      "columns_per_record": 10
    },
    "inner_table": {
      "table_name": "tables/10K_left_select10/rights/2KR_10C_select10_left5_right5.csv",
      "num_records": 2000,
      "columns_per_record": 10
    },
    "num_emitted_records": 1000
  },
  {
    "join_type": {
      "join_name": "SimpleHashJoin",
      "left_block_size": 0,
      "right_block_size": 0
    },
    "execution_time_nanos": 13808611,
    "outer_table": {
      "table_name": "tables/10K_left_select10/10KR_10C.csv",
      "num_records": 10000,
      "columns_per_record": 10
    },
    "inner_table": {
      "table_name": "tables/10K_left_select10/rights/10KR_10C_select10_left5_right5.csv",
      "num_records": 10000,
      "columns_per_record": 10
    },
    "num_emitted_records": 1000
  },
  ...
  {
    "join_type": {
      "join_name": "NLJoin",
      "left_block_size": 0,
      "right_block_size": 0
    },
    "execution_time_nanos": 8010942079,
    "outer_table": {
      "table_name": "tables/10K_left_select10/10KR_10C.csv",
      "num_records": 10000,
      "columns_per_record": 10
    },
    "inner_table": {
      "table_name": "tables/10K_left_select10/rights/10KR_10C_select10_left5_right5.csv",
      "num_records": 10000,
      "columns_per_record": 10
    },
    "num_emitted_records": 1000
  },
  ...
  ...
]
```

Note the Block-Nested-Loops Join will be run with a combination of different block sizes. Further, the profiling script calls `cargo run` under the hood. Specifically, this is also a valid run

```cargo run [left_table] [right_tables] [json_outfile] [left_block_size] [right_block_size] [join_algo]```

## Table Generation Instructions
   To generate a table, go to `src/bin/generate/main.rs` and change the config structs, then run with
   ```cargo run --bin generate```

   You must create the appropriate `tables/joinN/` and `tables/joinN/rights/` directories if they do not
   exist.

## Notes

- only David can set the 1 reviewer limit for the next two weeks (to my knowledge). Atm I think its prolly fine to just make the development branch protectted. Here are the instructions I googled:
    - Navigate to your project's Settings > Repository.
    - Expand Protected branches, and scroll to Protect a branch.
    - Select a Branch or wildcard you'd like to protect.  
    - Select the user levels Allowed to merge and Allowed to push.
    - https://docs.github.com/en/github/administering-a-repository/about-protected-branches

- If you make a pull request, link it to the issue by putting "resolves #<issue number>" in the title of the PR, or one of the other words. Link to documentation: https://docs.github.com/en/github/managing-your-work-on-github/linking-a-pull-request-to-an-issue



