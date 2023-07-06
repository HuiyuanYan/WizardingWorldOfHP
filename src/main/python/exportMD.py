# -*- coding: utf-8 -*-
import pandas as pd

nodes_file_path = 'nodes.xlsx'

edges_file_path = 'edges.xlsx'

character_info_dir = 'characterInfo/'

head_info = '---\ndg-publish: true\n---\n'

output_dir = 'markdown/'

picture_base_url = 'http://rxbg5ysja.bkt.gdipper.com/'

picture_format = '.png'

adj_dict = dict()
def read_nodes(file_path):
    nodes_data = pd.read_excel(file_path)
    for index,row in nodes_data.iterrows():
        node = row['Id']
        adj_dict[node] = []

def read_edges(file_path):
    edges_data = pd.read_excel(file_path)
    for index,row in edges_data.iterrows():
        source = row["Source"]
        target = row["Target"]
        adj_dict[source].append(target)

def generate_markdown():
    for key,val in adj_dict.items():
        print(f'generating markdown file of {key}...')
        context = head_info
        #添加人物图片超链接
        character = key.replace(' ','_')
        context += f'![{key}](http://rxbg5ysja.bkt.gdipper.com/{character}{picture_format})\n'
        # 读取人物信息
        context += '# Introduction\n'
        with open(f'{character_info_dir}{key}.txt','r',encoding='utf-8') as info_file:
            info_str = info_file.read()
            context += info_str
            info_file.close()

        # 添加相关人物的双向链接
        context += '# Related Character\n'
        for neighbor in val:
            context += f'[[{neighbor}]]\n'

        with open(f'{output_dir}{key}.md','w',encoding='utf-8') as write_file:
            write_file.write(context)
            print('finish\n')






read_nodes(nodes_file_path)
read_edges(edges_file_path)
generate_markdown()
