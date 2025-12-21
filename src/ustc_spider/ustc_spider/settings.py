# settings.py
import os

# --- 1. 基础项目信息 ---
BOT_NAME = 'ustc_spider'

SPIDER_MODULES = ['ustc_spider.spiders']
NEWSPIDER_MODULE = 'ustc_spider.spiders'

# --- 2. 伪装与反爬策略 ---
# 伪装成 Chrome 浏览器
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

# 不遵守 robots.txt (必须 False，否则很多校内网爬不到)
ROBOTSTXT_OBEY = False

# 禁用 Cookies (防止被服务器识别会话，同时节省内存)
COOKIES_ENABLED = False

# --- 3. 并发与速度控制 ---
# 全站爬取数据量大，适当开启并发
CONCURRENT_REQUESTS = 16

# 下载延迟 (秒): 0.5秒既能保证速度，又不至于太快把对方服务器打挂
# 如果遇到封禁，请将此值调大至 1 或 2
DOWNLOAD_DELAY = 0.5

# 下载超时 (秒): 防止卡在某些加载不出来的页面
DOWNLOAD_TIMEOUT = 15

# --- 4. 核心：数据流管道配置 (Pipelines) ---
# 数字越小，优先级越高，越先执行。
ITEM_PIPELINES = {
   # 阶段一：文件下载 (优先级 1 - 最高)
   # 必须最先执行，确保文件下载完成后，才把本地路径传给后续步骤
   'ustc_spider.pipelines.MyFilesPipeline': 1,
   
   # 阶段二：数据处理与入库 (优先级 300)
   # 在这里进行 Jieba 分词和写入 HBase
   'ustc_spider.pipelines.HBasePipeline': 300,
}

# --- 5. 文件下载专用配置 ---
# 告诉 Scrapy Item 中哪个字段是“文件下载链接”
FILES_URLS_FIELD = 'file_urls'
# 告诉 Scrapy 下载结果（本地路径）填回哪个字段
FILES_RESULT_FIELD = 'files'

# 文件存储绝对路径 (根据你的环境设置)
# 确保此文件夹有写权限，如果没有，代码会自动尝试创建
FILES_STORE = '/home/qin/USTC-BigData-Search/src/ustc_spider/downloads'

# 文件过期时间：90天内抓到相同的 URL 不会重新下载
FILES_EXPIRES = 90

# 允许 302 重定向 (非常重要！很多下载链接通过中间页跳转)
MEDIA_ALLOW_REDIRECTS = True

# --- 6. HBase 数据库配置 ---
# 对应启动命令: hbase thrift start -p 9090 -f -c
HBASE_HOST = '127.0.0.1'  # 建议使用 IP
HBASE_PORT = 9090         # Thrift 端口
HBASE_TABLE = 'ustc_web_data'

# --- 7. 日志配置 (可选) ---
# 只显示 INFO 及以上级别的日志，减少控制台刷屏
LOG_LEVEL = 'INFO'

DUPEFILTER_CLASS = 'scrapy.dupefilters.RFPDupeFilter'  # 启用框架去重
# ---------- 深度 2 层上限 ----------
DEPTH_LIMIT = 2

# ---------- 并发与速度 ----------
CONCURRENT_REQUESTS = 500
CONCURRENT_REQUESTS_PER_DOMAIN = 8
DOWNLOAD_DELAY = 0.2
AUTOTHROTTLE_ENABLED = True

# ---------- 禁止空闲早停 ----------
CLOSESPIDER_IDLE_NO_ITEMS = 0
CLOSESPIDER_TIMEOUT = 0

# ---------- 队列防内存爆炸 ----------
SCHEDULER_DISK_QUEUE = 'scrapy.squeues.PickleFifoDiskQueue'
SCHEDULER_MEMORY_QUEUE = 'scrapy.squeues.LifoMemoryQueue'