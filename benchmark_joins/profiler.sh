#!/bin/bash

# Expected usage of the script is that you
# are in the directory:
# 	6.830FinalProject/benchmark_joins
#
# And you specify the directory of generated data, like:
#		./profiler.sh tables/10K_left_select10


# 1) Parse arguments

# Extract the name of the experiment we want to run
# Example: tables/10K_left_select10
exp_name=$1
if [[ -z $exp_name ]]; then
	echo "Require experiment name!";
	exit 1;
fi

# Get left table
left_table="$(ls $exp_name/*.csv | tail -1)"

# Get right tables, separated by ,
right_tables="$(ls $exp_name/rights/*.csv | xargs | sed -e 's/ /;/g')"

# Make output directory
base_exp_name="$(basename $exp_name)"
outdir="experiments/$base_exp_name"
mkdir -p $outdir

# Wipe experiment contents of directory
rm $outdir/*.json

# Create output file
outfile="$outdir/$base_exp_name.json"
touch $outfile

# Now profile on tables
cargo run $left_table $right_tables $outfile