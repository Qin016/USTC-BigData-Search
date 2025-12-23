import happybase
import jieba
import json
import logging
from collections import defaultdict
from datetime import datetime
import math

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 停用词表
STOP_WORDS = {
    "的", "了", "和", "是", "就", "都", "而", "及", "与", "着",
    "或", "一个", "没有", "我们", "你们", "他们", "它", "在", "有",
    "个", "这", "那", "为", "之", "大", "来", "以", "中", "上", "下",
    "到", "说", "要", "去", "能", "会", "可", "也", "很", "真", "让",
    "自己", "什么", "怎么", "哪里", "这里", "那里", "但是", "因为", "所以",
    "如果", "虽然", "不仅", "而且", "或者", "还是", "以及", "关于", "对于",
    "根据", "按照", "通过", "由于", "为了", "除了", "包含", "包括", "其中",
    "例如", "比如", "等等", "以及", "并且", "或者", "或是", "要么", "既",
    "非", "即", "将", "对", "由", "向", "被", "给", "把", "次", "从",
    "自", "当", "并", "但", "而", "所", "诚", "之", "其", "或", "亦",
    "方", "即", "若", "则", "虽", "已", "故", "至", "及", "与", "且",
    "等", "应", "该", "此", "这些", "那些", "一些", "一点", "一切", "任何",
    "所有", "凡是", "各个", "各位", "各种", "各自", "某", "某某", "某些",
    "某个", "其它", "其他", "其余", "另外", "另", "别", "别的", "别人",
    "别处", "唯", "唯有", "只是", "不过", "只要", "只有", "除非", "尽管",
    "不管", "无论", "不论", "任", "任凭", "即使", "即便", "哪怕", "倘若",
    "假若", "假如", "要是", "如", "如果", "如若", "若", "若是", "果真",
    "果", "一", "二", "三", "四", "五", "六", "七", "八", "九", "十"
}

class USTCSearchEngine:
    def __init__(self, host='127.0.0.1', port=9090):
        self.host = host
        self.port = port
        self.data_table_name = 'ustc_web_data'
        self.index_table_name = 'ustc_keyword_index'
        self.connection = None
        self.data_table = None
        self.index_table = None
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
            self.data_table = self.connection.table(self.data_table_name)
            self.index_table = self.connection.table(self.index_table_name)
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

    def calculate_bm25(self, tf, doc_len=500, avg_len=500, k1=1.5, b=0.75):
        """
        计算 BM25 分数
        公式: score = (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * (doc_len / avg_len)))
        """
        return (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * (doc_len / avg_len)))

    def get_time_decay(self, date_str):
        """
        计算时间衰减
        解析 info:date (格式如 2024-12-23)。每早一年，权重乘以 0.9。
        """
        if not date_str:
            return 1.0
        
        try:
            # 尝试解析日期
            if isinstance(date_str, bytes):
                date_str = date_str.decode('utf-8', errors='ignore')
            
            # 简单处理可能的时间格式
            dt = datetime.strptime(date_str.strip(), "%Y-%m-%d")
            now = datetime.now()
            
            days_diff = (now - dt).days
            years_diff = days_diff / 365.0
            
            if years_diff < 0:
                years_diff = 0
                
            return 0.9 ** years_diff
        except Exception:
            return 1.0

    def search(self, query, top_k=20):
        """
        执行搜索 (双路混合检索: 倒排索引 + 标题扫描)
        """
        self._ensure_connection()
        raw_words = list(jieba.cut_for_search(query))
        query_words = [w for w in raw_words if w not in STOP_WORDS and len(w.strip()) > 0]
        
        if not query_words: return []

        # doc_id -> {score_info}
        combined_candidates = defaultdict(lambda: {'index_score': 0.0, 'scan_score': 0.0, 'cached_row': None})

        # --- 路径 A: 倒排索引召回 (40%) ---
        for word in query_words:
            row = self.index_table.row(word.encode('utf-8'))
            if row:
                for col_key, val_bytes in row.items():
                    doc_id = col_key.decode('utf-8').split(':', 1)[1]
                    try:
                        val_json = json.loads(val_bytes.decode('utf-8'))
                        tf = val_json.get('w', 0.0)
                        combined_candidates[doc_id]['index_score'] += self.calculate_bm25(tf)
                    except: pass

        # --- 路径 B: 主表标题扫描召回 (60%) ---
        # 移除对非 ASCII 字符的限制，支持中文标题扫描
        try:
            search_word = query_words[0]
            # 构造 Filter 字符串并编码为 utf-8
            filter_str = f"SingleColumnValueFilter('info', 'title', =, 'substring:{search_word}')"
            filter_bytes = filter_str.encode('utf-8')
            
            # 扫描主表 (限制 1000 条以平衡性能)
            scan_results = self.data_table.scan(
                filter=filter_bytes, 
                limit=10000, 
                columns=[b'info:title', b'info:type', b'files:path', b'info:date', b'info:url', b'info:parent_url']
            )
            
            for doc_id_bytes, row in scan_results:
                doc_id = doc_id_bytes.decode('utf-8')
                # 标题直接命中给予 15 分基础分
                combined_candidates[doc_id]['scan_score'] = 15.0
                combined_candidates[doc_id]['cached_row'] = row

        except Exception as e:
            logging.warning(f"Path B scan failed (ignoring): {e}")

        if not combined_candidates: return []

        # --- 批量获取详情 (Batch Fetch) ---
        # 找出还未缓存数据的文档 ID
        need_fetch_ids = [did for did, info in combined_candidates.items() if info['cached_row'] is None]
        
        # 分批获取，每批 100 个
        batch_size = 100
        for i in range(0, len(need_fetch_ids), batch_size):
            batch_ids = need_fetch_ids[i : i + batch_size]
            batch_ids_bytes = [did.encode('utf-8') for did in batch_ids]
            
            try:
                rows = dict(self.data_table.rows(
                    batch_ids_bytes, 
                    columns=[b'info:title', b'info:type', b'files:path', b'info:date', b'info:url', b'info:parent_url']
                ))
                
                for did_bytes, row in rows.items():
                    did = did_bytes.decode('utf-8')
                    if did in combined_candidates:
                        combined_candidates[did]['cached_row'] = row
            except Exception as e:
                logging.error(f"Batch fetch failed for batch {i}: {e}")

        # --- 统一打分与结果构建 ---
        final_list = []
        
        for doc_id, info in combined_candidates.items():
            row = info['cached_row']
            if not row: continue

            # 1. 基础分融合
            base_score = (info['index_score'] * 0.4) + (info['scan_score'] * 0.6)
            
            # 2. 附件加权 (File Boost)
            has_files = False
            files_path_bytes = row.get(b'files:path')
            doc_type = row.get(b'info:type', b'web')
            
            if doc_type == b'file':
                has_files = True
            elif files_path_bytes:
                try:
                    files = json.loads(files_path_bytes)
                    if files and len(files) > 0: has_files = True
                except: pass
            
            file_boost = 1.5 if has_files else 1.0
            
            # 3. 时间衰减 (Time Decay)
            time_decay = self.get_time_decay(row.get(b'info:date', b''))
            
            # Final Score Formula
            final_score = base_score * file_boost * time_decay
            
            # 字段处理
            title = row.get(b'info:title', '无标题'.encode('utf-8')).decode('utf-8', 'ignore')
            url = row.get(b'info:url', b'').decode('utf-8', 'ignore')
            parent_url = row.get(b'info:parent_url', b'').decode('utf-8', 'ignore')
            
            # URL 回退逻辑
            if not url and parent_url:
                url = parent_url

            file_paths = []
            if files_path_bytes:
                try:
                    file_paths = json.loads(files_path_bytes)
                except: pass

            final_list.append({
                'doc_id': doc_id,
                'title': title,
                'url': url,
                'score': round(final_score, 2),
                'type': 'file' if doc_type == b'file' else 'web',
                'date': row.get(b'info:date', b'').decode('utf-8', 'ignore'),
                'file_paths': file_paths,
                'parent_url': parent_url,
                'snippet': '' # 稍后填充
            })

        # --- 排序 (仅按 FinalScore 降序) ---
        final_list.sort(key=lambda x: x['score'], reverse=True)

        # --- 去重逻辑 (Deduplication) ---
        # 策略：保留分数最高的，去除 Title 和 URL 完全一致的重复项
        unique_list = []
        seen_keys = set()
        
        for item in final_list:
            # 构造去重键：(Title, URL)
            # 只有当标题和链接都完全一致时，才视为重复
            t = item['title'].strip()
            u = item['url'].strip()
            
            # 如果是文件，且 URL 是 parent_url，可能会有多个文件共享同一个 URL
            # 此时不能仅凭 URL 去重，必须结合 Title
            key = (t, u)
            
            if key not in seen_keys:
                seen_keys.add(key)
                unique_list.append(item)
        
        final_list = unique_list
        
        # --- 补全正文摘要 (Top K) ---
        top_final = final_list[:top_k]
        if top_final:
            top_ids_bytes = [d['doc_id'].encode('utf-8') for d in top_final]
            try:
                contents = dict(self.data_table.rows(top_ids_bytes, columns=[b'content:text']))
                for item in top_final:
                    raw_content = contents.get(item['doc_id'].encode('utf-8'), {}).get(b'content:text', b'')
                    # 清洗换行符并截取
                    clean_content = raw_content.decode('utf-8', 'ignore').replace('\n', ' ').replace('\r', ' ')
                    item['snippet'] = clean_content[:200] + "..."
            except Exception as e:
                logging.error(f"Fetch content failed: {e}")

        return top_final

    def close(self):
        if self.connection:
            self.connection.close()

if __name__ == "__main__":
    engine = USTCSearchEngine()
    # 测试搜索
    test_query = "计算机学院"
    results = engine.search(test_query)
    for res in results:
        type_tag = "[PDF/DOC]" if res['type'] == 'file' else "[网页]"
        print(f"{type_tag} {res['title']} (Score: {res['score']})")
        print(f"Link: {res['url']}")
        print(f"Snippet: {res['snippet']}\n")
