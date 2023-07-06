import pandas as pd
from py2neo import Graph, Node, Relationship

# Neo4j数据库连接配置
uri = "bolt://localhost:7687"  # Neo4j数据库的URI
username = "neo4j"  # 数据库用户名
password = "12345678"  # 数据库密码

# 读取nodes.xlsx文件
nodes_data = pd.read_excel("output/excel/nodes.xlsx")
# 读取edges.xlsx文件
edges_data = pd.read_excel("output/excel/edges.xlsx")

# 连接到Neo4j数据库
graph = Graph(uri=uri, user=username, password=password)

# 创建节点
for index, row in nodes_data.iterrows():
    node = Node(row["Label"],Id=row["Id"])
    graph.create(node)

# 创建无向边
for index, row in edges_data.iterrows():
    source = row["Source"]
    target = row["Target"]
    source_node = graph.nodes.match(Id=source).first()
    target_node = graph.nodes.match(Id=target).first()
    if source_node and target_node:
        relationship = Relationship(source_node, " ", target_node)
        graph.create(relationship)
    else:
        print(f"找不到节点 {source} 或 {target}，跳过创建边")

print("数据导入完成")