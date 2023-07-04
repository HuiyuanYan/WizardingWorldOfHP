import pandas as pd
import random

# 输入路径
input_label_path = 'output/task5/task5_ouput.txt'
input_graph_path = 'output/filter/filter_output.txt'

# 输出路径
output_node_path = 'output/excel/nodes.xlsx'
output_edge_path = 'output/excel/edges.xlsx'

# 是否输出边权重
output_weight = True

# 同一标签节点之间的边加权权值
same_label_weight_low = 20
same_label_weight_high = 30

# 不同标签节点之间的边加权权值
diff_label_weight_low = 1
diff_label_weight_high = 5
# 读取节点标签文本文件，生成节点 DataFrame
node_label_dict = dict()
nodes_data = []
with open(input_label_path, 'r') as file1:
    for line in file1:
        line = line.strip()
        if line:
            node_id, _, node_label = line.split('#')
            node_label_dict[node_id] = node_label
            nodes_data.append({'Id': node_id, 'Label': node_label})

nodes_df = pd.DataFrame(nodes_data)


edges_data = []
with open(input_graph_path, 'r') as file2:
    for line in file2:
        line = line.strip()
        if line:
            node_id, neighbors = line.split('\t')
            neighbors = neighbors[1:-1]
            for neighbor_weight in neighbors.split('|'):
                
                neighbor, weight = neighbor_weight.split('@')
                if output_weight:
                    if node_label_dict[node_id] == node_label_dict[neighbor]:
                        edges_data.append({'Source': node_id, 'Target': neighbor,'Weight':random.randint(same_label_weight_low,same_label_weight_high)})
                    else:
                        edges_data.append({'Source': node_id, 'Target': neighbor,'Weight':random.randint(diff_label_weight_low,diff_label_weight_high)})
                else:
                    edges_data.append({'Source': node_id, 'Target': neighbor})


edges_df = pd.DataFrame(edges_data)


# 导出为 Excel 文件
nodes_df.to_excel(output_node_path, index=False)
edges_df.to_excel(output_edge_path, index=False)
