# USTC-BigData-Search

基于 Scrapy + HBase + Flask + RAG 的中国科学技术大学网站智能搜索系统。

## 项目简介

本项目实现了一个完整的网络爬虫和智能搜索系统：
- **数据采集**: 使用 Scrapy 爬虫框架爬取 USTC 各学院网站数据
- **数据存储**: 使用 HBase 分布式数据库存储网页和附件信息
- **智能检索**: 基于关键词倒排索引的搜索引擎
- **RAG 增强**: 集成大语言模型，提供智能问答服务
- **Web 展示**: Flask 前端展示搜索结果和流式 AI 回答

## 项目文件树

```
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
│   │   ├── requirements.txt           # Python 依赖
│   │   ├── static/                    # 静态资源 (CSS, JS)
│   │   └── templates/                 # HTML 模板
│   └── ustc_spider/    # 爬虫代码 (Scrapy)
│       ├── scrapy.cfg                 # Scrapy 配置文件
│       ├── sites.yaml                 # 爬取站点配置
│       └── ustc_spider/               # 爬虫核心代码
└── README.md           # 项目说明
```

## 快速开始

### 1. 环境准备

#### 1.1 系统要求
- Python 3.8+
- Java 8+ (HBase 依赖)
- Ollama (可选，用于本地 LLM 服务)

#### 1.2 安装 HBase

**Linux环境:**
```bash
# 下载并解压
wget https://downloads.apache.org/hbase/2.5.x/hbase-2.5.x-bin.tar.gz
tar -xzf hbase-2.5.x-bin.tar.gz
cd hbase-2.5.x

# 启动 HBase
bin/start-hbase.sh
```

#### 1.3 启动 HBase Thrift 服务

**必须使用 Framed Transport + Compact Protocol 模式:**
```
bin/hbase thrift start -f -c
```

验证服务启动成功 (默认端口 9090):
```bash
netstat -an | grep 9090       # Linux
```

#### 1.4 安装 Python 依赖

**爬虫依赖:**
```bash
cd src/ustc_spider
pip install scrapy happybase jieba thrift
```

**Web 服务依赖:**
```bash
cd src/rag
pip install -r requirements.txt
# 包含: flask, happybase, jieba, langchain, langchain-ollama
```

#### 1.5 安装 Ollama (可选)

如果需要使用本地大语言模型:
```bash
# 下载安装: https://ollama.ai
ollama pull qwen2.5:14b    # 或其他模型
ollama serve
```

### 2. 配置爬虫

#### 2.1 配置要爬取的网站

编辑 [src/ustc_spider/sites.yaml](src/ustc_spider/sites.yaml):
```yaml
# 添加要爬取的网站
- name: "cs"
  url: "http://cs.ustc.edu.cn/"
- name: "math"
  url: "https://math.ustc.edu.cn/"
# ... 更多站点
```

#### 2.2 配置 HBase 连接

编辑 [src/ustc_spider/ustc_spider/settings.py](src/ustc_spider/ustc_spider/settings.py):
```python
# HBase 配置
HBASE_HOST = '127.0.0.1'  # HBase Thrift 服务地址
HBASE_PORT = 9090         # Thrift 端口
HBASE_TABLE = 'ustc_web_data'

# 文件下载目录
FILES_STORE = r'C:\Users\Lenovo\Desktop\大数据分析\USTC-BigData-Search\src\ustc_spider\downloads'
```

### 3. 运行爬虫

#### 3.1 启动爬虫
```bash
cd src/ustc_spider
scrapy crawl universal_spider
```

爬虫会自动:
1. 读取 `sites.yaml` 中的网站列表
2. 爬取网页内容和附件 (PDF、DOC、XLS 等)
3. 使用 Jieba 进行中文分词和关键词提取
4. 将数据存储到 HBase 的 `ustc_web_data` 表

#### 3.2 监控爬虫进度

查看 HBase 数据:
```bash
# 进入 HBase Shell
hbase shell

# 查看表
list

# 查看数据量
count 'ustc_web_data'

# 查看具体记录
scan 'ustc_web_data', {LIMIT => 5}
```

### 4. 构建倒排索引

爬虫完成后，构建关键词倒排索引以加速搜索:

```bash
cd src/etl
python build_inverted_index.py
```

该脚本会:
1. 扫描 `ustc_web_data` 表中的所有关键词
2. 构建倒排索引到 `ustc_keyword_index` 表
3. 每个关键词对应一个文档列表 (含相关性分数)

### 5. 启动 Web 服务

#### 5.1 配置搜索引擎

编辑 [src/rag/search_engine.py](src/rag/search_engine.py)，确认 HBase 配置:
```python
HBASE_HOST = '127.0.0.1'
HBASE_PORT = 9090
```

#### 5.2 启动 Flask 应用
```bash
cd src/rag
python app.py
```

服务默认运行在 `http://localhost:5000`

#### 5.3 访问 Web 界面

打开浏览器访问: [http://localhost:5000](http://localhost:5000)

功能:
- **搜索框**: 输入关键词搜索
- **结果展示**: 显示相关网页和附件
- **AI 回答**: 基于搜索结果生成智能答案 (需要 Ollama)
- **文件预览**: 点击 PDF/DOC 等附件可在线预览或下载


## 数据库表结构

### ustc_web_data (主数据表)
| 列族 | 列名 | 说明 |
|------|------|------|
| info | title | 网页标题 |
| info | url | 网页 URL |
| info | keywords | 关键词列表 (JSON) |
| info | type | 类型 (web/file) |
| info | project | 所属项目/学院 |
| info | date | 爬取时间 |
| content | text | 网页正文 |
| files | paths | 附件本地路径 (JSON) |
| files | parent_url | 文件来源页面 |

### ustc_keyword_index (倒排索引表)
| 列族 | 列名 | 说明 |
|------|------|------|
| p | postings | 包含该关键词的文档列表 (JSON) |


## 开发说明

### 项目技术栈
- **爬虫**: Scrapy 2.x
- **数据库**: HBase 2.5.x
- **分词**: Jieba
- **Web 框架**: Flask
- **大模型**: Langchain + Ollama
- **前端**: HTML + CSS + JavaScript

