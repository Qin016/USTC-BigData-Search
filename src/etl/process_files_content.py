#!/usr/bin/env python3
"""
ETL 脚本：处理本地下载的附件，提取文本并写入 HBase（每个附件作为一行）

位置: src/etl/process_files_content.py

功能概要:
- 扫描 HBase 表 `ustc_web_data` 中有 `files:path` 的父网页记录
- 遍历文件，使用 Tika 提取全文（parser.from_file）
- 根据优先级生成智能标题
- 使用 jieba TF-IDF 提取关键词（含权重）
- 将每个文件以 RowKey=MD5(file_bytes) 写入 HBase

注意：脚本使用 framed/compact 连接 HBase（happybase），请确保 HBase thrift 服务已按要求启动。
"""

import os
import sys
import json
import logging
import hashlib
import re
from typing import Optional

import happybase
from tika import parser
import jieba.analyse


# ---------- 配置 ----------
HBASE_HOST = os.environ.get('HBASE_HOST', 'localhost')
HBASE_PORT = int(os.environ.get('HBASE_PORT', '9090'))
TABLE_NAME = os.environ.get('HBASE_TABLE', 'ustc_web_data')

# 本地文件存储目录（相对于 Scrapy FILES_STORE）
FILES_STORE = os.environ.get('FILES_STORE', '/home/plx/USTC-BigData-Search/src/ustc_spider/downloads')

# 解析与写入限制
MAX_CONTENT_STORE = 50000  # 存入 HBase 的文本最大长度
KEYWORDS_TOPK = 20

# Tika 服务器端点（可选）
# 如果你希望使用已有的 Tika Server（通过 docker 或独立进程启动），
# 可以在环境变量中设置 TIKA_SERVER_ENDPOINT，例如 http://localhost:9998
if os.environ.get('TIKA_SERVER_ENDPOINT'):
    os.environ['TIKA_SERVER_ENDPOINT'] = os.environ.get('TIKA_SERVER_ENDPOINT')


# ---------- 日志 ----------
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger('etl')


def connect_hbase(host: str, port: int) -> Optional[happybase.Connection]:
    """建立到 HBase 的连接，使用 framed/compact（严格要求）。"""
    try:
        conn = happybase.Connection(host=host, port=port, timeout=20000, transport='framed', protocol='compact')
        conn.open()
        logger.info(f"Connected to HBase at {host}:{port}")
        return conn
    except Exception:
        logger.exception("Failed to connect to HBase")
        return None


def clean_text(s: str) -> str:
    """清洗文本，去除不可见字符并将换行、制表替换为空格，压缩多空格。"""
    if not s:
        return ''
    # 去掉 NUL 字符
    s = s.replace('\x00', ' ')
    # 替换换行和制表为空格
    s = s.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    # 移除其他不可见控制字符
    s = re.sub(r'[\x00-\x1f\x7f]', ' ', s)
    # 压缩多个空格
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def compute_md5_of_file(path: str) -> Optional[str]:
    try:
        m = hashlib.md5()
        with open(path, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                m.update(chunk)
        return m.hexdigest()
    except Exception:
        logger.exception(f"Failed to compute MD5 for {path}")
        return None


def smart_title(metadata: dict, extracted_text: str, parent_title: str) -> str:
    """根据规则生成智能标题。
    优先级：metadata['title'] -> extracted_text 第一行前50字符 -> f"【附件】{parent_title}"。
    """
    # metadata 字典里可能包含 'title'
    try:
        if metadata:
            meta_title = metadata.get('title') or metadata.get('Title')
            if meta_title and isinstance(meta_title, str) and meta_title.strip():
                return meta_title.strip()
    except Exception:
        pass

    if extracted_text:
        # 取第一行的前50个字符
        first_line = extracted_text.split('\n')[0].strip()
        if first_line:
            return first_line[:50]

    return f"【附件】{parent_title or ''}".strip()


def extract_keywords(text: str, topk: int = KEYWORDS_TOPK):
    try:
        tags = jieba.analyse.extract_tags(text, topK=topk, withWeight=True,
                                          allowPOS=('n', 'nz', 'v', 'vd', 'vn', 'l', 'a', 'd'))
        return [{'word': w, 'weight': float(wt)} for w, wt in tags]
    except Exception:
        logger.exception('Failed to extract keywords')
        return []


def process_file(abs_path: str, rel_path: str, parent_url: str, parent_title: str, table) -> None:
    logger.info(f"Processing file: {abs_path}")
    try:
        if not os.path.exists(abs_path):
            logger.warning(f"File not found: {abs_path}")
            return

        # compute row key by MD5 of file bytes
        row_key_md5 = compute_md5_of_file(abs_path)
        if not row_key_md5:
            logger.warning(f"Skipping file due to MD5 failure: {abs_path}")
            return

        # parse with Tika
        parsed = parser.from_file(abs_path)
        raw_content = parsed.get('content') or ''
        if not raw_content or not raw_content.strip():
            logger.warning(f"Empty content extracted for {abs_path}, skipping")
            return

        # clean
        cleaned = clean_text(raw_content)
        logger.info(f"Extracted {len(cleaned)} characters from {abs_path}")

        # metadata
        metadata = parsed.get('metadata') or {}

        # smart title
        title = smart_title(metadata, cleaned, parent_title)

        # keywords
        keywords = extract_keywords(cleaned)

        # assemble data for HBase
        data = {
            b'info:type': b'file',
            b'info:title': title.encode('utf-8', 'ignore'),
            b'info:parent_url': (parent_url or '').encode('utf-8', 'ignore'),
            b'content:text': cleaned[:MAX_CONTENT_STORE].encode('utf-8', 'ignore'),
            b'info:keywords': json.dumps(keywords, ensure_ascii=False).encode('utf-8'),
            # Store the relative path so we can download it later
            b'files:path': json.dumps([rel_path], ensure_ascii=False).encode('utf-8')
        }

        # write row
        table.put(row_key_md5, data)
        logger.info(f"Wrote file row {row_key_md5} to HBase (title: {title})")

    except Exception:
        logger.exception(f"Error processing file {abs_path}")


def scan_and_process(table):
    logger.info('Starting table scan for rows with files:path...')
    # We scan for files:path and get parent info
    try:
        # columns: files:path, info:url, info:title
        for key, data in table.scan(columns=[b'files:path', b'info:url', b'info:title']):
            try:
                files_path_bytes = data.get(b'files:path')
                if not files_path_bytes:
                    continue

                try:
                    files_list = json.loads(files_path_bytes.decode('utf-8'))
                except Exception:
                    logger.warning(f"Failed to decode files:path for row {key}: {files_path_bytes}")
                    continue

                if not files_list:
                    continue

                parent_url = (data.get(b'info:url') or b'').decode('utf-8', 'ignore')
                parent_title = (data.get(b'info:title') or b'').decode('utf-8', 'ignore')

                logger.info(f"Row {key.decode('utf-8') if isinstance(key, bytes) else key}: Found {len(files_list)} files. Parent title: {parent_title}")

                for rel_path in files_list:
                    # construct absolute path
                    abs_path = os.path.join(FILES_STORE, rel_path)
                    process_file(abs_path, rel_path, parent_url, parent_title, table)

            except Exception:
                logger.exception(f"Failed to process row {key}")

    except Exception:
        logger.exception('Failed to scan HBase table')


def main():
    conn = connect_hbase(HBASE_HOST, HBASE_PORT)
    if not conn:
        logger.error('Cannot proceed without HBase connection')
        return

    try:
        table = conn.table(TABLE_NAME)
    except Exception:
        logger.exception(f"Failed to access table {TABLE_NAME}")
        conn.close()
        return

    try:
        scan_and_process(table)
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == '__main__':
    main()