# libraries
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import json



# Data

f = open('./10K_left_select10.json','r')
data = json.load(f)

print(data[0])


df = pd.DataFrame(
    {'x_values': range(1, 11), 'y1_values': np.random.randn(10), 'y2_values': np.random.randn(10) + range(1, 11),
     'y3_values': np.random.randn(10) + range(11, 21)})

# collect necessary values:

x_values = []
numJoins = len(data[0])
joinNames = []
allTableData = {'x':[]}

#construct table of tables for number of joins.
for x in range(numJoins):
    temp = data[0][x]
    allTableData[temp['join_type']['join_name']] = []
    joinNames.append(temp['join_type']['join_name'])
    print(joinNames)

print(allTableData.keys())
for x in range(len(data)): #number of tables
    denomTemp = data[x][0]
    ratio = (denomTemp['inner_table']['num_records'])/(denomTemp['outer_table']['num_records'])
    allTableData['x'].append(ratio)
    for o in range(numJoins):
        temp = data[x][o]
        allTableData[temp['join_type']['join_name']].append(temp['execution_time_nanos']/1000000)




allTableData['x'] = allTableData['x'][1:]+[allTableData['x'][0]]

for y in range(numJoins):
    allTableData[joinNames[y]] = allTableData[joinNames[y]][1:] + [allTableData[joinNames[y]][0]]
    plt.plot(allTableData['x'],allTableData[joinNames[y]], marker='o', label=joinNames[y])

plt.legend()

# # multiple line plots
# plt.plot('x_values', 'y1_values', data=df, marker='o', markerfacecolor='blue', markersize=12, color='skyblue',
#          linewidth=4)
# plt.plot('x_values', 'y2_values', data=df, marker='', color='olive', linewidth=2)
# plt.plot('x_values', 'y3_values', data=df, marker='', color='olive', linewidth=2, linestyle='dashed', label="toto")
# # show legend
# plt.legend()

print(allTableData)

# show graph
plt.draw()
#plt.show()
plt.savefig('allJoins.png')