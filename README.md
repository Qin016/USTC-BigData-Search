# USTC-BigData-Search
## 初版文件树
USTC-BigData-Search/
├── data/               # 存放临时下载的文件、本地向量库索引
├── docker/             # 存放 docker-compose.yml 和配置文件
├── src/                # 源代码
│   ├── spider/         # 爬虫代码 (Scrapy)
│   ├── etl/            # 数据清洗与入库代码 (Spark)
│   ├── llm/            # RAG 与大模型相关代码
│   └── web/            # 前端展示代码 (Streamlit/Flask)
├── notebooks/          # 用于测试代码片段的 Jupyter Notebooks
├── requirements.txt    # Python 依赖
└── README.md           # 项目说明