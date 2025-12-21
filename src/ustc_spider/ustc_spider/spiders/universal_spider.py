import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
import yaml
import os
import re
from urllib.parse import urlparse
from ustc_spider.items import GeneralSpiderItem

class UniversalSpider(CrawlSpider):
    name = 'universal_spider'
    
    # --- 改进点 1: 更智能的规则 ---
    # Scrapy 是从上到下匹配规则的。
    rules = (
        # 规则A：优先追踪看起来像“分页”或“列表”的链接 (包含 list, page, index_ 等关键词)
        Rule(LinkExtractor(allow=(r'list', r'page', r'index_\d+', r'view'), 
                           deny=(r'login', r'logout', r'javascript')), 
             callback='parse_item', follow=True),
             
        # 规则B：兜底规则，追踪域名下的其他链接，确保不漏掉详情页
        # 注意：对于全站爬取，这就足够覆盖“所有页面”了
        Rule(LinkExtractor(allow=(), deny=(r'login', r'\.jpg', r'\.png')), 
             callback='parse_item', follow=True),
    )

    def __init__(self, *args, **kwargs):
        super(UniversalSpider, self).__init__(*args, **kwargs)
        self.project_configs = []
        self.start_urls = []
        self.allowed_domains = []
        
        # 加载配置 (逻辑保持不变，为了节省篇幅略去部分打印)
        file_path = os.path.join(os.getcwd(), 'sites.yaml')
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                self.project_configs = yaml.safe_load(f)
                if self.project_configs:
                    for site in self.project_configs:
                        url = site['url']
                        self.start_urls.append(url)
                        domain = urlparse(url).netloc
                        self.allowed_domains.append(domain)
        except Exception as e:
            print(f"Error loading config: {e}")

    def parse_item(self, response):
        # 1. 识别项目归属
        project_name = 'unknown'
        current_domain = urlparse(response.url).netloc
        for config in self.project_configs:
            if urlparse(config['url']).netloc in current_domain:
                project_name = config['name']
                break
        
        # --- 改进点 2: 尝试从 URL 中识别页码，打印进度 ---
        # 很多网站 URL 类似 list_10.htm 或 ?page=10
        page_match = re.search(r'[_\?](?:page|p|index)[=_](\d+)', response.url)
        page_info = f" [Page: {page_match.group(1)}]" if page_match else ""

        item = GeneralSpiderItem()
        item['url'] = response.url
        item['project'] = project_name
        item['title'] = response.xpath('//title/text()').get(default='').strip()
        
        # 提取正文
        text_nodes = response.xpath('//body//text()').getall()
        # 简单清洗：去除过多换行
        item['parsed_text'] = ''.join([t.strip() for t in text_nodes if t.strip()])[:50000]
        item['html_content'] = "" # 占位

        # --- 改进点 3: 提取文件并准备下载 ---
        file_urls = []
        # 扩展了文件后缀支持
        extensions = ('.pdf', '.doc', '.docx', '.xls', '.xlsx', '.zip', '.rar', '.7z', '.ppt', '.pptx')
        
        # 既查 a 标签的 href，也可以查 iframe 的 src (部分老网站)
        links = response.css('a::attr(href), iframe::attr(src)').getall()
        
        for link in links:
            # 清洗链接，去除空白
            link = link.strip()
            lower_link = link.lower()
            if lower_link.endswith(extensions):
                abs_url = response.urljoin(link)
                file_urls.append(abs_url)
        
        # 将去重后的 URL 列表传给 item
        item['file_urls'] = list(set(file_urls))
        
        # 仅当有效时才 yield
        if item['title']:
            print(f"[{project_name}]{page_info} Crawled: {item['title'][:30]}... | Files found: {len(item['file_urls'])}")
            yield item