from nebula3.gclient.net import ConnectionPool
from nebula3.Config import Config
import numpy as np
import pandas as pd
import re
import time

# 预处理函数：处理字符串以便可以安全地在nGQL查询中使用
def preprocess_string(s):
    if pd.isna(s) or s == "无":
        return ""
    # 转为字符串并去除引号，防止SQL注入
    return str(s).replace("'", "''").strip()
# 生成VID的函数
def generate_vid(entity_type, entity_name):
    if entity_name in vid_mapping:
        return vid_mapping[entity_name]
    # 为该实体创建新的VID
    new_vid = f"{entity_type}{counter[entity_type]:02d}"
    vid_mapping[entity_name] = new_vid
    counter[entity_type] += 1
    return new_vid

config = Config() # 定义一个配置
config.max_connection_pool_size = 10 # 设置最大连接数
connection_pool = ConnectionPool() # 初始化连接池
# 如果给定的服务器是ok的，返回true，否则返回false
ok = connection_pool.init([('127.0.0.1', 9669)], config)
data_df = pd.read_excel("/root/VscodeProject/PythonProject/nebula_data/实体.xlsx", 
                      usecols=['实体', '分数', '关联论文'])


with connection_pool.session_context('root', 'nebula') as session:
    # 使用test_paperdata空间
    session.execute('USE test_paperdata')
    session.execute('CREATE TAG IF NOT EXISTS sensitive_entity(entity_name string, sensitive int)')
    session.execute('CREATE EDGE IF NOT EXISTS related_to_paper()')
    #time.sleep(10)  # 等待索引构建完成
    # 追踪已插入的顶点，避免重复
    inserted_vertices = {
        'entity': set(),
    }
        # 创建顶点ID映射和计数器
    vid_mapping = {}  # 存储实体名称到VID的映射
    counter = {
        'sensitive_entity': 1
    }
    # 批量插入的语句列表
    nGQL_statements = []
    
    # 遍历每一行数据
    for index, row in data_df.iterrows():
        entity_name = preprocess_string(row['实体'])
        sensitive = int(row['分数']) if not pd.isna(row['分数']) else 0
        related_papers_str = preprocess_string(row['关联论文'])
        
        # 处理关键技术实体顶点
        if entity_name and entity_name not in inserted_vertices['entity']:
            entity_vid = generate_vid('sensitive_entity', entity_name)
# 修正语法：注意引号的正确使用方式
            nGQL_statements.append(f'''
            INSERT VERTEX IF NOT EXISTS sensitive_entity(entity_name, sensitive) 
            VALUES "{entity_vid}":("{entity_name}", {sensitive})
            ''')
            inserted_vertices['entity'].add(entity_name)
        
        # 处理关联论文 - 分割可能包含多个论文的字符串
        if related_papers_str:
            # 清理格式：移除多余的引号、方括号和其他干扰字符
            clean_papers_str = related_papers_str.replace('[', '').replace(']', '').replace('"', '').replace("'", "")
            
            # 使用方括号内的任意标点符号分割论文标题
            related_papers = [paper.strip() for paper in re.split('[,]', clean_papers_str) if paper.strip()]
            
            not_found_papers = []
            for paper_title in related_papers:
                
                # 检查论文是否存在于图数据库中
                check_result = session.execute(f'LOOKUP ON paper WHERE paper.title == "{paper_title}" YIELD id(VERTEX)')

                if check_result.is_succeeded() and check_result.rows():
                    # 从查询结果中正确提取论文VID
                    paper_vid = check_result.column_values('id(VERTEX)')[0]  # 使用column_values获取VID
                    # 论文存在，添加关联关系
                    nGQL_statements.append(f'''
                    INSERT EDGE IF NOT EXISTS related_to_paper() VALUES "{entity_vid}"->{paper_vid}:()
                    ''')
                else:
                    # 论文不存在，记录下来
                    not_found_papers.append(paper_title)
            
            # 如果有未找到的论文，打印出来并保存到文件
            if not_found_papers:
                print(f"实体 '{entity_name}' 关联的以下论文未找到: {not_found_papers}")
                
                # 将未找到的论文信息保存到文件
                with open('/root/VscodeProject/PythonProject/nebula_data/not_found_papers.txt', 'a', encoding='utf-8') as f:
                    f.write(f"实体: {entity_name}\n")
                    for paper in not_found_papers:
                        f.write(f"  - {paper}\n")
                    f.write("\n")
    
    # 执行所有语句
    print(f"共计 {len(nGQL_statements)} 条语句等待执行")
    
    # 批量执行插入语句，每500条执行一次
    batch_size = 500
    for i in range(0, len(nGQL_statements), batch_size):
        batch = nGQL_statements[i:i+batch_size]
        for stmt in batch:
            try:
                result = session.execute(stmt)
                if not result.is_succeeded():
                    print(f"执行失败: {stmt}")
                    print(f"错误信息: {result.error_msg()}")
            except Exception as e:
                print(f"执行异常: {stmt}")
                print(f"异常信息: {str(e)}")
        print(f"已执行 {min(i+batch_size, len(nGQL_statements))} / {len(nGQL_statements)} 条语句")
    
    print("关键技术数据导入完成")