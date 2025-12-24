#!/usr/bin/env python3
# src/etl/build_inverted_index.py

import happybase
import json
import logging
import sys
import os

# Configuration
HBASE_HOST = os.environ.get('HBASE_HOST', 'localhost')
HBASE_PORT = int(os.environ.get('HBASE_PORT', '9090'))
SOURCE_TABLE = 'ustc_web_data'
TARGET_TABLE = 'ustc_keyword_index'
BATCH_SIZE = 1000

# Logging setup
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger('inverted_index_builder')

# Basic Chinese Stop Words
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

def connect_hbase():
    """Connect to HBase with required transport and protocol settings."""
    try:
        connection = happybase.Connection(
            host=HBASE_HOST,
            port=HBASE_PORT,
            timeout=20000,
            transport='framed',
            protocol='compact'
        )
        connection.open()
        logger.info(f"Connected to HBase at {HBASE_HOST}:{HBASE_PORT}")
        return connection
    except Exception as e:
        logger.error(f"Failed to connect to HBase: {e}")
        sys.exit(1)

def create_target_table(connection):
    """Create target table if it does not exist."""
    try:
        tables = [t.decode('utf-8') for t in connection.tables()]
        if TARGET_TABLE not in tables:
            logger.info(f"Creating table {TARGET_TABLE}...")
            connection.create_table(
                TARGET_TABLE,
                {'p': dict()}  # Column family 'p'
            )
            logger.info(f"Table {TARGET_TABLE} created.")
        else:
            logger.info(f"Table {TARGET_TABLE} already exists.")
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        sys.exit(1)

def build_index(connection):
    """Scan source table and build inverted index in target table."""
    source_table = connection.table(SOURCE_TABLE)
    target_table = connection.table(TARGET_TABLE)
    
    logger.info(f"Scanning {SOURCE_TABLE}...")
    
    batch = target_table.batch(batch_size=BATCH_SIZE)
    count = 0
    processed_docs = 0
    
    try:
        # Scan only necessary columns
        scanner = source_table.scan(columns=[b'info:keywords', b'info:type'])
        
        for row_key, data in scanner:
            doc_id = row_key.decode('utf-8') if isinstance(row_key, bytes) else row_key
            
            # Get document type (default to 'web')
            doc_type_bytes = data.get(b'info:type')
            doc_type = doc_type_bytes.decode('utf-8') if doc_type_bytes else 'web'
            
            # Get keywords
            keywords_bytes = data.get(b'info:keywords')
            if not keywords_bytes:
                continue
                
            try:
                keywords_list = json.loads(keywords_bytes.decode('utf-8'))
            except json.JSONDecodeError:
                logger.warning(f"Invalid JSON in info:keywords for doc {doc_id}")
                continue
                
            if not isinstance(keywords_list, list):
                continue
                
            # Process each keyword
            for kw_item in keywords_list:
                # Handle both dict format (from jieba with weights) and simple string list
                if isinstance(kw_item, dict):
                    word = kw_item.get('word', '')
                    weight = kw_item.get('weight', 1.0)
                elif isinstance(kw_item, str):
                    word = kw_item
                    weight = 1.0
                else:
                    continue
                
                # Filter logic
                if not word or len(word) < 2:
                    continue
                if word in STOP_WORDS:
                    continue
                
                # Prepare data for inverted index
                # RowKey: Keyword
                # Column: p:{DocID}
                # Value: JSON {'w': weight, 't': type}
                
                index_row_key = word.encode('utf-8')
                column = f'p:{doc_id}'.encode('utf-8')
                value = json.dumps({'w': round(weight, 4), 't': doc_type}).encode('utf-8')
                
                batch.put(index_row_key, {column: value})
                count += 1
            
            processed_docs += 1
            if processed_docs % 100 == 0:
                logger.info(f"Processed {processed_docs} documents...")
                
        # Send any remaining mutations
        batch.send()
        logger.info(f"Index build complete. Processed {processed_docs} documents. Total index entries: {count}")
        
    except Exception as e:
        logger.error(f"Error building index: {e}")
    finally:
        # Ensure batch is closed/sent if exception occurred (though send() handles it usually)
        pass

def main():
    connection = connect_hbase()
    try:
        create_target_table(connection)
        build_index(connection)
    finally:
        connection.close()

if __name__ == "__main__":
    main()
