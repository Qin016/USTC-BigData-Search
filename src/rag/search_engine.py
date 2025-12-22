import happybase
import jieba
import json
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class USTCSearchEngine:
    def __init__(self, host='127.0.0.1', port=9090):
        self.host = host
        self.port = port
        self.table_name = 'ustc_web_data'
        self.connection = None
        self.table = None
        self._connect()

    def _connect(self):
        """建立 HBase 连接"""
        try:
            self.connection = happybase.Connection(
                host=self.host,
                port=self.port,
                timeout=10000,
                transport='framed',
                protocol='compact'
            )
            self.connection.open()
            self.table = self.connection.table(self.table_name)
            logging.info("✅ Successfully connected to HBase")
        except Exception as e:
            logging.error(f"❌ Failed to connect to HBase: {e}")

    def _ensure_connection(self):
        """确保连接可用，如果断开则重连"""
        try:
            # 尝试获取表列表来测试连接
            self.connection.tables()
        except Exception:
            logging.warning("⚠️ Connection lost, reconnecting...")
            self._connect()

    def search(self, query, top_k=5):
        """
        执行搜索
        :param query: 用户查询词
        :param top_k: 返回结果数量
        :return: 结果列表
        """
        self._ensure_connection()
        
        if not self.table:
            logging.error("HBase table not initialized.")
            return []

        # 1. 对 Query 进行分词
        query_words = list(jieba.cut_for_search(query))
        logging.info(f"Query words: {query_words}")

        results = []

        # 2. 扫描 HBase
        # 注意：全表扫描在数据量大时效率较低，生产环境建议使用 ElasticSearch 或 Solr 建立倒排索引
        try:
            # 仅获取需要的列族/列，减少网络传输
            # 假设我们需要 info:title, info:url, info:keywords, content:text, files:path
            for key, data in self.table.scan():
                score = 0
                
                # 解码数据，处理可能的 None
                def get_str(col):
                    return data.get(col, b'').decode('utf-8', errors='ignore')

                title = get_str(b'info:title')
                url = get_str(b'info:url')
                keywords_json = get_str(b'info:keywords')
                content = get_str(b'content:text')
                file_paths_json = get_str(b'files:path')

                # 解析关键词
                try:
                    keywords = json.loads(keywords_json) if keywords_json else []
                except json.JSONDecodeError:
                    keywords = []

                # 解析文件路径
                try:
                    file_paths = json.loads(file_paths_json) if file_paths_json else []
                except json.JSONDecodeError:
                    file_paths = []

                # 3. 打分逻辑
                
                # A. 标题匹配 (权重 5)
                for word in query_words:
                    if word in title:
                        score += 5
                
                # B. 关键词匹配 (权重 2 * stored_weight)
                # 遍历文档的关键词，看是否在查询词中
                for kw in keywords:
                    kw_word = kw.get('word', '')
                    kw_weight = float(kw.get('weight', 1.0))
                    
                    # 如果文档关键词出现在用户的查询分词中
                    if kw_word in query_words:
                        score += 2 * kw_weight
                    
                    # 也可以反向匹配：如果查询词出现在文档关键词中（模糊匹配）
                    # 这里严格按照 "关键词匹配" 理解，通常指精确匹配
                
                if score > 0:
                    # 生成摘要 (简单截取前 200 字符)
                    summary = content[:200].replace('\n', ' ') + '...' if content else "暂无正文内容"
                    
                    results.append({
                        'title': title,
                        'url': url,
                        'keywords': [k['word'] for k in keywords], # 提取关键词列表
                        'file_paths': file_paths,
                        'summary': summary,
                        'content': content, # RAG 需要全文
                        'score': score
                    })

            # 4. 排序并返回 Top K
            results.sort(key=lambda x: x['score'], reverse=True)
            return results[:top_k]

        except Exception as e:
            logging.error(f"Error during search scan: {e}")
            return []

    def close(self):
        if self.connection:
            self.connection.close()
