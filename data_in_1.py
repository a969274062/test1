from nebula3.gclient.net import ConnectionPool
from nebula3.Config import Config
import numpy as np
import pandas as pd
import json
import unicodedata
import re

def read_paper_info(paper):
    try:

        # Extract the required sections
        paper_info = {
            "题目": paper.get("题目", ""),
            "作者": paper.get("作者", []),
            "基金资助": paper.get("基金资助", []),
            "参考文献": paper.get("参考文献", [])
        }
        
        return paper_info
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    
data_df = pd.read_excel("/root/VscodeProject/PythonProject/nebula_data/论文信息表_1.xlsx", 
                      usecols=['论文标题', '期刊名称', '摘要', '关键词', '发表时间', '专辑', '专题', '分类号', '下载量', '页数', '引用量'])
with open('/root/VscodeProject/PythonProject/nebula_data/output.json', 'r', encoding='utf-8') as file:
    papers = json.load(file)
# 处理数据并插入到数据库
# 预处理函数：处理字符串以便可以安全地在nGQL查询中使用
def preprocess_string(s):
    if pd.isna(s) or s == "无":
        return ""
    # 转为字符串并去除引号，防止SQL注入
    return str(s).replace("'", "''").strip()

# 处理页数字段，提取数字部分
def extract_page_number(page_str):
    if pd.isna(page_str):
        return "0"
    # 使用正则表达式提取数字部分
    match = re.search(r'\d+', str(page_str))
    if match:
        return match.group()
    return "0"


# 分割关键词
def split_keywords(keywords_str):
    if pd.isna(keywords_str) or keywords_str == "无":
        return []
    return [kw.strip() for kw in re.split('[；;，,、]', keywords_str) if kw.strip()]

# 分割分类号
def split_classification(class_str):
    if pd.isna(class_str) or class_str == "无":
        return []
    return [cls.strip() for cls in re.split('[；;，,、]', class_str) if cls.strip()]

# 分割单位
def split_organizations(org_str):
    if pd.isna(org_str) or org_str == "无":
        return []
    return [org.strip() for org in re.split('[；;，,、]', org_str) if org.strip()]

# 分割专题
def split_topics(topic_str):
    if pd.isna(topic_str) or topic_str == "无":
        return []
    return [topic.strip() for topic in re.split('[；;，,、]', topic_str) if topic.strip()]

# 分割专辑
def split_albums(album_str):
    if pd.isna(album_str) or album_str == "无":
        return []
    return [album.strip() for album in re.split('[；;，,、]', album_str) if album.strip()]

# 添加字符串清理函数
def clean_text_for_nebula(text):
    try:
        # 尝试解码和重新编码以删除无效字符
        text = text.encode('utf-8', 'ignore').decode('utf-8')
        
        # 规范化 Unicode
        text = unicodedata.normalize('NFKC', text)
        
        # 替换引号，防止 SQL 注入和语法错误
        text = text.replace("'", "\\'").replace('"', '\\"')
        
        # 删除可能导致问题的不可见字符
        text = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]', '', text)
        
        return text
    except Exception as e:
        print(f"清理文本时出错: {e}")
        # 如果处理失败，返回安全的空字符串
        return ""

config = Config() # 定义一个配置
config.max_connection_pool_size = 10 # 设置最大连接数
connection_pool = ConnectionPool() # 初始化连接池
# 如果给定的服务器是ok的，返回true，否则返回false
ok = connection_pool.init([('127.0.0.1', 9669)], config)

with connection_pool.session_context('root', 'nebula') as session:
    # 创建图空间test_paperdata
    try:
        # 检查图空间是否已存在
        check_space_result = session.execute('SHOW SPACES')
        existing_spaces = [record.values()[0].as_string() for record in check_space_result]
        
        if 'test_paperdata' not in existing_spaces:
            print("创建新的图空间: test_paperdata")
            # 创建图空间
            create_space_stmt = '''
            CREATE SPACE IF NOT EXISTS test_paperdata (
                vid_type = FIXED_STRING(256),
                partition_num = 10,
                replica_factor = 1
            )
            '''
            session.execute(create_space_stmt)
            # 提交事务
            session.execute('COMMIT')
            # 提交统计信息
            session.execute('SUBMIT JOB STATS')
            print("等待图空间创建完成...")
            import time
            time.sleep(20)  # 等待图空间创建完成
            
            # 切换到新创建的图空间
            session.execute('USE test_paperdata')
            
            # 创建索引和Tags/Edges
            print("创建标签（Tags）和边类型（Edge Types）...")
            
            # 创建标签
            session.execute('CREATE TAG IF NOT EXISTS paper(title string, abstract string, release_time string, download_times int, page int, quote_times int)')
            session.execute('CREATE TAG IF NOT EXISTS journal(name string)')
            session.execute('CREATE TAG IF NOT EXISTS author(name string)')
            session.execute('CREATE TAG IF NOT EXISTS organization(name string)')
            session.execute('CREATE TAG IF NOT EXISTS key_word(name string)')
            session.execute('CREATE TAG IF NOT EXISTS classification_number(name string)')
            session.execute('CREATE TAG IF NOT EXISTS topic(name string)')
            session.execute('CREATE TAG IF NOT EXISTS album(name string)')
            session.execute('CREATE TAG IF NOT EXISTS fund(name string, fund_number string)')

            # 创建边类型
            session.execute('CREATE EDGE IF NOT EXISTS which_journal()')
            session.execute('CREATE EDGE IF NOT EXISTS which_author()')
            session.execute('CREATE EDGE IF NOT EXISTS which_organization()')
            session.execute('CREATE EDGE IF NOT EXISTS which_key_word()')
            session.execute('CREATE EDGE IF NOT EXISTS which_classification_number()')
            session.execute('CREATE EDGE IF NOT EXISTS which_topic()')
            session.execute('CREATE EDGE IF NOT EXISTS which_album()')
            session.execute('CREATE EDGE IF NOT EXISTS which_fund()')
            session.execute('CREATE EDGE IF NOT EXISTS which_reference()')
            session.execute('CREATE EDGE IF NOT EXISTS taking_office()')

            
            # 创建索引
            print("创建索引...")
            session.execute('CREATE TAG INDEX IF NOT EXISTS i_paper ON paper()')
            print("等待索引构建完成...")
            time.sleep(20)  # 等待索引构建完成
            
            print("图空间 test_paperdata 创建成功！")
            result = session.execute('SHOW TAGS')
            print(result)
        else:
            print("图空间 test_paperdata 已存在，将直接使用。")
            session.execute('USE test_paperdata')
    except Exception as e:
        print(f"创建图空间时出错: {str(e)}")

    # 追踪已插入的顶点，避免重复
    inserted_vertices = {
        'journal': set(),
        'author': set(),
        'organization': set(),
        'key_word': set(),
        'classification': set(),
        'topic': set(),
        'album': set(),
        'fund': set(),
        'paper': set()
    }
    
    # 创建顶点ID映射和计数器
    vid_mapping = {}  # 存储实体名称到VID的映射
    counter = {
        'paper': 1,
        'journal': 1,
        'author': 1,
        'organization': 1,
        'key_word': 1,
        'classification': 1,
        'topic': 1,
        'album': 1,
        'fund': 1
    }
    
    # 生成VID的函数
    def generate_vid(entity_type, entity_name):
        if entity_name in vid_mapping:
            return vid_mapping[entity_name]
        
        # 为该实体创建新的VID
        new_vid = f"{entity_type}{counter[entity_type]:02d}"
        vid_mapping[entity_name] = new_vid
        counter[entity_type] += 1
        return new_vid
    
    # 批量插入的语句列表
    nGQL_statements = []
    
    # 遍历每一行数据
    for (index,row),paper_row in zip(data_df.iterrows(),papers):
        paper_title = preprocess_string(row['论文标题'])
        paper_abstract = clean_text_for_nebula(preprocess_string(row['摘要']))
        paper_publish_time = preprocess_string(row['发表时间'])
        paper_downloads = preprocess_string(row['下载量'])
        paper_pages = extract_page_number(row['页数'])
        paper_citations = row['引用量']
        
        journal_name = preprocess_string(row['期刊名称'])
        
        # 生成论文顶点ID
        paper_vid = generate_vid('paper', paper_title)
        
        # 处理论文顶点
        nGQL_statements.append(f'''
        INSERT VERTEX IF NOT EXISTS paper(title, abstract, release_time, download_times, page, quote_times) 
        VALUES "{paper_vid}":('{paper_title}', '{paper_abstract}', '{paper_publish_time}', {int(paper_downloads)}, {int(paper_pages)}, {int(paper_citations)})
        ''')
        
        # 处理期刊顶点
        if journal_name and journal_name not in inserted_vertices['journal']:
            journal_vid = generate_vid('journal', journal_name)
            nGQL_statements.append(f'''
            INSERT VERTEX IF NOT EXISTS journal(name) VALUES "{journal_vid}":('{journal_name}')
            ''')
            inserted_vertices['journal'].add(journal_name)
        
        # 添加论文与期刊的关系
        if journal_name:
            journal_vid = vid_mapping[journal_name]
            nGQL_statements.append(f'''
            INSERT EDGE IF NOT EXISTS which_journal() VALUES "{paper_vid}"->"{journal_vid}":()
            ''')
        paper_info = read_paper_info(paper_row)
        # 处理基金资助
        if '基金资助' in paper_info and paper_info['基金资助']:
            for fund in paper_info['基金资助']:
                fund_name = preprocess_string(fund.get('项目名称', ''))
                fund_number = preprocess_string(fund.get('项目号', ''))
                
                if fund_name and fund_name not in inserted_vertices['fund']:
                    fund_vid = generate_vid('fund', fund_name)
                    nGQL_statements.append(f'''
                    INSERT VERTEX IF NOT EXISTS fund(name, fund_number) VALUES "{fund_vid}":('{fund_name}', '{fund_number}')
                    ''')
                    inserted_vertices['fund'].add(fund_name)
                
                if fund_name:
                    fund_vid = vid_mapping[fund_name]
                    nGQL_statements.append(f'''
                    INSERT EDGE IF NOT EXISTS which_fund() VALUES "{paper_vid}"->"{fund_vid}":()
                    ''')
        # 处理作者和单位
        if '作者' in paper_info and paper_info['作者']:
            authors = paper_info['作者']
            for author in authors:
                author_name = preprocess_string(author.get('姓名', ''))
                author_affiliation = author.get('单位', [])
                
                if author_name and author_name not in inserted_vertices['author']:
                    author_vid = generate_vid('author', author_name)
                    nGQL_statements.append(f'''
                    INSERT VERTEX IF NOT EXISTS author(name) VALUES "{author_vid}":('{author_name}')
                    ''')
                    inserted_vertices['author'].add(author_name)
                
                if author_name:
                    author_vid = vid_mapping[author_name]
                    nGQL_statements.append(f'''
                    INSERT EDGE IF NOT EXISTS which_author() VALUES "{paper_vid}"->"{author_vid}":()
                    ''')
                # 处理单位
                for org_name in author_affiliation:
                    org_name = preprocess_string(org_name)
                    if org_name and org_name not in inserted_vertices['organization']:
                        org_vid = generate_vid('organization', org_name)
                        nGQL_statements.append(f'''
                        INSERT VERTEX IF NOT EXISTS organization(name) VALUES "{org_vid}":('{org_name}')
                        ''')
                        inserted_vertices['organization'].add(org_name)
                    
                    if org_name:
                        org_vid = vid_mapping[org_name]
                        nGQL_statements.append(f'''
                        INSERT EDGE IF NOT EXISTS which_organization() VALUES "{paper_vid}"->"{org_vid}":()
                        ''')
                        nGQL_statements.append(f'''
                        INSERT EDGE IF NOT EXISTS taking_office() VALUES "{author_vid}"->"{org_vid}":()
                        ''')
        
        # 处理关键词
        keywords = split_keywords(row['关键词'])
        for keyword in keywords:
            if keyword and keyword not in inserted_vertices['key_word']:
                keyword_vid = generate_vid('key_word', keyword)
                nGQL_statements.append(f'''
                INSERT VERTEX IF NOT EXISTS key_word(name) VALUES "{keyword_vid}":('{keyword}')
                ''')
                inserted_vertices['key_word'].add(keyword)
            
            if keyword:
                keyword_vid = vid_mapping[keyword]
                nGQL_statements.append(f'''
                INSERT EDGE IF NOT EXISTS which_key_word() VALUES "{paper_vid}"->"{keyword_vid}":()
                ''')
        
        # 处理分类号
        classifications = split_classification(row['分类号'])
        for cls in classifications:
            if cls and cls not in inserted_vertices['classification']:
                cls_vid = generate_vid('classification', cls)
                nGQL_statements.append(f'''
                INSERT VERTEX IF NOT EXISTS classification_number(name) VALUES "{cls_vid}":('{cls}')
                ''')
                inserted_vertices['classification'].add(cls)
            
            if cls:
                cls_vid = vid_mapping[cls]
                nGQL_statements.append(f'''
                INSERT EDGE IF NOT EXISTS which_classification_number() VALUES "{paper_vid}"->"{cls_vid}":()
                ''')
        
        # 处理专题
        topics = split_topics(row['专题'])
        for topic in topics:
            if topic and topic not in inserted_vertices['topic']:
                topic_vid = generate_vid('topic', topic)
                nGQL_statements.append(f'''
                INSERT VERTEX IF NOT EXISTS topic(name) VALUES "{topic_vid}":('{topic}')
                ''')
                inserted_vertices['topic'].add(topic)
            
            if topic:
                topic_vid = vid_mapping[topic]
                nGQL_statements.append(f'''
                INSERT EDGE IF NOT EXISTS which_topic() VALUES "{paper_vid}"->"{topic_vid}":()
                ''')
        
        # 处理专辑
        albums = split_albums(row['专辑'])
        for album in albums:
            if album and album not in inserted_vertices['album']:
                album_vid = generate_vid('album', album)
                nGQL_statements.append(f'''
                INSERT VERTEX IF NOT EXISTS album(name) VALUES "{album_vid}":('{album}')
                ''')
                inserted_vertices['album'].add(album)
            
            if album:
                album_vid = vid_mapping[album]
                nGQL_statements.append(f'''
                INSERT EDGE IF NOT EXISTS which_album() VALUES "{paper_vid}"->"{album_vid}":()
                ''')
    
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
    
    print("元数据导入完成")
    # 提交事务
    session.execute('COMMIT')
    # 提交统计信息
    session.execute('SUBMIT JOB STATS')
    nGQL_statements = []  # 清空语句列表，准备下一步操作
    # 导入每篇论文的参考文献
    for (index,row),paper_row in zip(data_df.iterrows(),papers):
        paper_title = preprocess_string(row['论文标题'])
        paper_info = read_paper_info(paper_row)
        #处理参考文献
        if '参考文献' in paper_info and paper_info['参考文献']:
            references = paper_info['参考文献']
            not_found_papers = []
            for ref in references:
                ref_title = preprocess_string(ref.get('题目', ''))
                # 检查论文是否存在于图数据库中
                check_result1 = session.execute(f'LOOKUP ON paper WHERE paper.title == "{ref_title}" YIELD id(VERTEX)')
                # 检查论文是否存在于图数据库中
                check_result2 = session.execute(f'LOOKUP ON paper WHERE paper.title == "{paper_title}" YIELD id(VERTEX)')
                if check_result1.is_succeeded() and check_result1.rows() and check_result2.is_succeeded() and check_result2.rows():
                    # 从查询结果中正确提取论文VID
                    ref_vid = check_result1.column_values('id(VERTEX)')[0]  # 使用column_values获取VID
                    paper_vid = check_result2.column_values('id(VERTEX)')[0]
                    # 论文存在，添加关联关系
                    nGQL_statements.append(f'''
                    INSERT EDGE IF NOT EXISTS which_reference() VALUES {paper_vid}->{ref_vid}:()
                    ''')
                else:
                    # 论文不存在，记录下来
                    not_found_papers.append(ref_title)
                    # 如果有未找到的论文，打印出来并保存到文件
            if not_found_papers:
                #print(f"论文 '{paper_title}' 参考的以下论文未找到: {not_found_papers}")
                
                # 将未找到的论文信息保存到文件
                with open('/root/VscodeProject/PythonProject/nebula_data/not_found_papers.txt', 'a', encoding='utf-8') as f:
                    f.write(f"论文: {paper_title}\n")
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
    print("参考文献数据导入完成")
    # 提交事务
    session.execute('COMMIT')
    # 提交统计信息
    session.execute('SUBMIT JOB STATS')
    # 关闭连接池
    connection_pool.close()