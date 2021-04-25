import pandas as pd

# Read the files into two dataframes.
df1 = pd.read_csv('tables/test/10R_10C.csv')
df2 = pd.read_csv('tables/test/rights/10R_10C_select10_left5_right5.csv')

# Merge the two dataframes, using _ID column as key
df3 = pd.merge(df1, df2, left_on = 'col6', right_on = 'col6')

# Write it to a new CSV file
df3.to_csv('groundtruth/10K_left_select10/10K_10K_select10.csv')