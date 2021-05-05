#!/bin/bash

# Expected usage of the script is that you
# are in the directory:
# 	6.830FinalProject/benchmark_joins
#
# And you specify the directory of generated data, along with the joins, like:
#
#		./profiler.sh tables/10K_left_select10 hash,nl,bnl

# Extract the name of the experiment we want to run
# Example: tables/10K_left_select10
exp_name=$1
if [[ -z $exp_name ]]; then
	echo "Require experiment name!"
	exit 1
fi

# Extract the ordered list of algos to run
# Example: hash,nl,bnl
in_algos=$2
if [[ -z $in_algos ]]; then
	echo "Require comma separated algos!"
	exit 1
fi
algos="$(echo $in_algos | tr -s ',' ' ')"

# Extract number of trials to run each algo-tables-block combo
# Example: 3
num_trials=$3
if [[ -z $num_trials ]]; then
	echo "Require number of trials"
	exit 1
elif [[ $num_trials < 1 ]]; then
	echo "Expected positive number of trials, got $num_trials"
	exit 1
fi

# Choose block sizes
block_sizes=( 50 500 5000 )

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
echo "[]" > $outdir/$base_exp_name.json

# Create output file
outfile="$outdir/$base_exp_name.json"
touch $outfile

# Now profile on tables
for algo in ${algos[@]}; do
	if [[ $algo == "bnl" ]]; then
		# For block nested loops, run all block size combinations
		for lbs in ${block_sizes[@]}; do
			for rbs in ${block_sizes[@]}; do
				cargo run $left_table $right_tables $outfile $lbs $rbs $algo $num_trials
			done
		done
	else
		# For all non-BNL joins, run with some irrelevant number of blocks
		cargo run $left_table $right_tables $outfile 1 1 $algo $num_trials
	fi
done