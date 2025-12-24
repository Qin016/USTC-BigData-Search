# USTC-BigData-Search
## 项目文件树
USTC-BigData-Search/
├── data/               # 存放临时下载的文件、本地向量库索引
├── src/                # 源代码
│   ├── etl/            # 数据清洗与入库代码
│   │   ├── build_inverted_index.py    # 构建倒排索引
│   │   └── process_files_content.py   # 处理文件内容
│   ├── rag/            # RAG 服务与 Web 展示
│   │   ├── app.py                     # Flask 应用入口
│   │   ├── rag_service.py             # RAG 核心逻辑
│   │   ├── search_engine.py           # 搜索引擎接口
│   │   ├── static/                    # 静态资源 (CSS, JS)
│   │   └── templates/                 # HTML 模板
│   └── ustc_spider/    # 爬虫代码 (Scrapy)
│       ├── scrapy.cfg                 # Scrapy 配置文件
│       ├── sites.yaml                 # 爬取站点配置
│       └── ustc_spider/               # 爬虫核心代码
└── README.md           # 项目说明