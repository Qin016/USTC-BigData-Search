# pipelines.py
import happybase
import hashlib
import json
import logging
import os
import jieba.analyse
from urllib.parse import unquote, urlparse
from scrapy.pipelines.files import FilesPipeline
from scrapy.utils.project import get_project_settings

# --- 阶段一：文件下载管道 ---
class MyFilesPipeline(FilesPipeline):
    """
    负责将 file_urls 里的链接下载到本地。
    执行完毕后，会将本地路径填入 item['files']。
    """
    def file_path(self, request, response=None, info=None, *, item=None):
        # 1. 获取项目名 (用于创建子文件夹)
        project_name = item.get('project', 'default')
        
        # 2. 提取原始文件名
        url_path = urlparse(request.url).path
        decoded_path = unquote(url_path)
        filename = os.path.basename(decoded_path)
        
        # 3. 容错处理
        if not filename:
            filename = hashlib.md5(request.url.encode()).hexdigest() + ".file"
            
        # 最终保存在: FILES_STORE/project_name/filename
        return f'{project_name}/{filename}'


# --- 阶段二：处理与入库管道 ---
class HBasePipeline:
    """
    功能：
    1. 连接 HBase
    2. 对文本进行 TF-IDF 关键词提取 (含权重)
    3. 将元数据、关键词、文件路径存入 HBase
    """
    def __init__(self):
        self.settings = get_project_settings()
        self.host = self.settings.get('HBASE_HOST', '127.0.0.1')
        self.port = self.settings.getint('HBASE_PORT', 9090)
        self.table_name = self.settings.get('HBASE_TABLE', 'ustc_web_data')
        self.connection = None
        self.table = None

    def open_spider(self, spider):
        """爬虫启动时建立 HBase 连接"""
        try:
            # 必须匹配 hbase thrift start -f -c (Framed Transport + Compact Protocol)
            self.connection = happybase.Connection(
                self.host, port=self.port, timeout=20000,
                transport='framed', protocol='compact'
            )
            self.connection.open()
            
            # 自动建表逻辑
            tables = [t.decode('utf-8') for t in self.connection.tables()]
            if self.table_name not in tables:
                self.connection.create_table(
                    self.table_name,
                    {
                        'info': dict(),      # 基础信息 (标题, URL, 关键词)
                        'content': dict(),   # 文本内容
                        'files': dict()      # 文件路径信息
                    }
                )
            self.table = self.connection.table(self.table_name)
            logging.info("✅ [HBase] Pipeline Ready.")
        except Exception as e:
            logging.error(f"❌ [HBase] Connection Failed: {e}")

    def close_spider(self, spider):
        if self.connection:
            self.connection.close()

    def process_item(self, item, spider):
        # 如果连接没成功，直接返回，防止报错崩溃
        if not self.table:
            return item

        try:
            # === 1. 数据准备 ===
            url = item['url']
            # RowKey 设计：使用 URL 的 MD5，确保唯一且长度固定
            row_key = hashlib.md5(url.encode('utf-8')).hexdigest()
            raw_text = item.get('parsed_text', '').replace('\x00', '')
            
            # === 2. 关键词提取与分析 (TF-IDF) ===
            # 提取前 20 个高频词，并保留权重 (withWeight=True)
            # 权重对于后续的“文档检索引擎”计算相关度非常重要
            keywords_data = []
            if raw_text:
                tags = jieba.analyse.extract_tags(
                    raw_text, topK=20, withWeight=True, 
                    allowPOS=('n', 'nz', 'v', 'vd', 'vn', 'l', 'a', 'd') # 仅提取实词
                )
                # 转换为 [{"word": "计算机", "weight": 1.23}, ...] 格式
                keywords_data = [{"word": tag[0], "weight": tag[1]} for tag in tags]

            # === 3. 获取本地文件路径 ===
            local_file_paths = []
            if 'files' in item and item['files']:
                for f in item['files']:
                    local_file_paths.append(f['path'])

            # === 4. 组装数据 ===
            data = {
                b'info:url': url.encode('utf-8'),
                b'info:title': item['title'].encode('utf-8'),
                b'info:project': item['project'].encode('utf-8'),
                b'info:date': item.get('date', '').encode('utf-8'),
                
                # 核心修改：存储带有权重的关键词 JSON
                b'info:keywords': json.dumps(keywords_data, ensure_ascii=False).encode('utf-8'),
                
                # 存入正文 (截取前30000字符避免过大)
                b'content:text': raw_text[:30000].encode('utf-8', 'ignore'),
                
                # 存入本地文件的路径列表
                b'files:path': json.dumps(local_file_paths).encode('utf-8')
            }

            # === 5. 写入 HBase ===
            self.table.put(row_key, data)
            
            # 日志展示
            file_count = len(local_file_paths)
            # 打印前3个关键词方便调试
            top_kw = ",".join([k['word'] for k in keywords_data[:3]])
            logging.info(f"💾 [Saved] {item['title'][:15]}... | Files: {file_count} | Keywords: {top_kw}")

        except Exception as e:
            logging.error(f"❌ [Error] {e}")

        return item