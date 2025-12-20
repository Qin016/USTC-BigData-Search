import happybase
import hashlib
import json
import logging
import os
from scrapy.pipelines.files import FilesPipeline
from scrapy.utils.project import get_project_settings

class MyFilesPipeline(FilesPipeline):
    def file_path(self, request, response=None, info=None, *, item=None):
        original_name = request.url.split('/')[-1]
        project_name = item.get('project', 'default')
        return f'{project_name}/{original_name}'

class HBasePipeline:
    def __init__(self):
        self.host = 'localhost'
        self.port = 9090
        self.table_name = 'ustc_web_data'

    def process_item(self, item, spider):
        connection = None
        try:
            # === 关键修改 ===
            # 使用 framed 传输和 compact 协议，匹配服务端的新启动参数
            # 这种组合处理大文本和跨语言通信最稳定
            connection = happybase.Connection(
                self.host, 
                port=self.port, 
                timeout=20000,  # 超时时间延长到 20秒
                transport='framed',  # 对应服务端的 -f
                protocol='compact'   # 对应服务端的 -c
            )
            table = connection.table(self.table_name)
            
            # 生成 RowKey
            url = item['url']
            row_key = hashlib.md5(url.encode('utf-8')).hexdigest()

            # 数据清洗 (进一步压缩长度，安全第一)
            raw_text = item.get('parsed_text', '')
            clean_text = raw_text.replace('\x00', '') # 去除空字节
            safe_text = clean_text[:10000] # 降级：只存前 10000 字，防止包过大

            data = {
                b'info:url': url.encode('utf-8'),
                b'info:title': item['title'].encode('utf-8'),
                b'info:project': item['project'].encode('utf-8'),
                b'info:date': item.get('date', '').encode('utf-8'),
                b'content:text': safe_text.encode('utf-8', 'ignore'),
            }

            if 'file_urls' in item and item['file_urls']:
                data[b'files:urls'] = json.dumps(item['file_urls']).encode('utf-8')
            
            if 'files' in item and item['files']:
                file_paths = [f['path'] for f in item['files']]
                data[b'files:path'] = json.dumps(file_paths).encode('utf-8')

            table.put(row_key, data)
            # 使用 logging 打印，在终端看更清晰
            logging.info(f"✅ [HBase] Saved: {item['title'][:20]}...")

        except Exception as e:
            # 记录详细错误堆栈
            logging.error(f"❌ [HBase] Error writing item {item.get('url')}: {e}")
        
        finally:
            if connection:
                try:
                    connection.close()
                except:
                    pass
        
        return item