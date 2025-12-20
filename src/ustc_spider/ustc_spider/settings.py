import os

# --- 基础设置 ---
BOT_NAME = 'ustc_spider'

SPIDER_MODULES = ['ustc_spider.spiders']
NEWSPIDER_MODULE = 'ustc_spider.spiders'

# 伪装成浏览器 (必须有)
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

# 不遵守 robots.txt (防止被拒绝访问)
ROBOTSTXT_OBEY = False

# --- 并发与延迟 ---
CONCURRENT_REQUESTS = 16
DOWNLOAD_DELAY = 1  # 每次请求等待1秒，防止被封IP

# --- 核心：Pipeline 配置 ---
ITEM_PIPELINES = {
   # 数字越小越先执行。
   # 1. 先执行下载附件的 Pipeline
   'ustc_spider.pipelines.MyFilesPipeline': 1, 
   # 2. 下载完后，执行写入 HBase 的 Pipeline
   'ustc_spider.pipelines.HBasePipeline': 300,
}

# --- 核心：文件下载设置 ---
# 1. 字段映射 (告诉 Scrapy 哪个字段是链接，下载结果存哪个字段)
FILES_URLS_FIELD = 'file_urls'
FILES_RESULT_FIELD = 'files'

# 2. 文件过期时间 (90天内不重复下载同一个文件)
FILES_EXPIRES = 90

# 3. 允许重定向 (关键！很多下载链接是 302 跳转)
MEDIA_ALLOW_REDIRECTS = True

# 4. 存储路径 (使用绝对路径，确保你能找到！)
# 路径指向: /home/qin/USTC-BigData-Search/src/ustc_spider/downloads
FILES_STORE = '/home/qin/USTC-BigData-Search/src/ustc_spider/downloads'

# --- HBase 配置 (供 Pipeline 使用) ---
HBASE_HOST = 'localhost'
HBASE_PORT = 9090
HBASE_TABLE = 'ustc_web_data'