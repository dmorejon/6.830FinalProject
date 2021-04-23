# 6.830FinalProject

## Benchmark Running Instructions

To run benchmarking, navigate to `6.830FinalProject/benchmark_joins`. From there, you can run 

```cargo run [table1_name] [table2_name] [outfile_json]```

And that will run the different joins, time them, and put the results in `outfile_json`. Specifically, this will **overrwite the contents of file**. For example, running 

```cargo run tables/small1.csv tables/small2.csv experiments/smalls.json```

will generate `experiments/smalls.json` and have content similar to:
```JSON
[
   {
      "join_type":{
         "join_name":"NLJoin",
         "left_block_size":0,
         "right_block_size":0
      },
      "execution_time_nanos":28922,
      "outer_table":{
         "table_name":"tables/small1.csv",
         "num_records":6,
         "columns_per_record":3
      },
      "inner_table":{
         "table_name":"tables/small2.csv",
         "num_records":6,
         "columns_per_record":3
      },
      "num_emitted_records":3
   },
   {
      "join_type":{
         "join_name":"NLJoin",
         "left_block_size":0,
         "right_block_size":0
      },
      "execution_time_nanos":12896,
      "outer_table":{
         "table_name":"tables/small2.csv",
         "num_records":6,
         "columns_per_record":3
      },
      "inner_table":{
         "table_name":"tables/small1.csv",
         "num_records":6,
         "columns_per_record":3
      },
      "num_emitted_records":3
   },
   {
      "join_type":{
         "join_name":"BNLJoin",
         "left_block_size":2,
         "right_block_size":2
      },
      "execution_time_nanos":157570,
      "outer_table":{
         "table_name":"tables/small1.csv",
         "num_records":6,
         "columns_per_record":3
      },
      "inner_table":{
         "table_name":"tables/small2.csv",
         "num_records":6,
         "columns_per_record":3
      },
      "num_emitted_records":3
   },
   {
      "join_type":{
         "join_name":"BNLJoin",
         "left_block_size":2,
         "right_block_size":2
      },
      "execution_time_nanos":116060,
      "outer_table":{
         "table_name":"tables/small2.csv",
         "num_records":6,
         "columns_per_record":3
      },
      "inner_table":{
         "table_name":"tables/small1.csv",
         "num_records":6,
         "columns_per_record":3
      },
      "num_emitted_records":3
   }
]
```

## Notes

- only David can set the 1 reviewer limit for the next two weeks (to my knowledge). Atm I think its prolly fine to just make the development branch protectted. Here are the instructions I googled:
    - Navigate to your project's Settings > Repository.
    - Expand Protected branches, and scroll to Protect a branch.
    - Select a Branch or wildcard you'd like to protect.  
    - Select the user levels Allowed to merge and Allowed to push.
    - https://docs.github.com/en/github/administering-a-repository/about-protected-branches

- If you make a pull request, link it to the issue by putting "resolves #<issue number>" in the title of the PR, or one of the other words. Link to documentation: https://docs.github.com/en/github/managing-your-work-on-github/linking-a-pull-request-to-an-issue



