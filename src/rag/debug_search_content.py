import happybase
import jieba
import sys
import os

# 添加当前目录到 sys.path 以便导入 search_engine
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from search_engine import USTCSearchEngine

def search_specific_content(keyword):
    print(f"正在检查内容: '{keyword}' ...\n")
    
    engine = USTCSearchEngine()
    
    # 1. 检查倒排索引
    print("--- 1. 检查倒排索引 (ustc_keyword_index) ---")
    # 对查询词进行分词，因为索引是按词存储的
    words = list(jieba.cut_for_search(keyword))
    print(f"分词结果: {words}")
    
    if engine.index_table:
        for w in words:
            row = engine.index_table.row(w.encode('utf-8'))
            if row:
                print(f"✅ 词条 '{w}' 存在于索引中，关联文档数: {len(row)}")
            else:
                print(f"❌ 词条 '{w}' 未在索引中找到")
    else:
        print("错误: 无法连接到索引表")

    # 2. 使用搜索引擎进行完整搜索
    print(f"\n--- 2. 执行完整搜索 (Top 5) ---")
    results = engine.search(keyword, top_k=5)
    
    if results:
        print(f"找到 {len(results)} 条结果:")
        for i, res in enumerate(results):
            print(f"\n结果 #{i+1}")
            print(f"标题: {res['title']}")
            print(f"类型: {res['type']}")
            print(f"分数: {res['score']}")
            print(f"URL: {res['url']}")
            print(f"摘要: {res['snippet'][:100]}...")
    else:
        print("未找到相关结果。")

    #3. (可选) 暴力扫描数据表标题 (仅当索引没找到时有用，用于调试)
    print(f"\n--- 3. 扫描数据表标题 (ustc_web_data) ---")
    count = 0
    if engine.data_table:
        # 只扫描 info:title 列
        scanner = engine.data_table.scan(columns=[b'info:title'])
        for key, data in scanner:
            title = data.get(b'info:title', b'').decode('utf-8', errors='ignore')
            if keyword in title:
                print(f"✅ 在文档标题中找到: {title} (RowKey: {key.decode()})")
                count += 1
                if count >= 5:
                    print("... (仅显示前5条)")
                    break
        if count == 0:
            print("在所有文档标题中未找到该关键词。")

    engine.close()

if __name__ == "__main__":
    target = "王江涛"
    if len(sys.argv) > 1:
        target = sys.argv[1]
    search_specific_content(target)
