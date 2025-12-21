import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
import yaml
import os
from urllib.parse import urlparse
from ustc_spider.items import GeneralSpiderItem

class UniversalSpider(CrawlSpider):
    name = 'universal_spider'
    
    # 定义通用规则：只要在允许的域名内，就提取所有链接继续爬取
    rules = (
        Rule(LinkExtractor(allow=()), callback='parse_item', follow=True),
    )

    def __init__(self, *args, **kwargs):
        super(UniversalSpider, self).__init__(*args, **kwargs)
        
        # 1. 初始化配置列表 (修复 AttributeError 的关键)
        self.project_configs = []
        self.start_urls = []
        self.allowed_domains = []

        # 2. 读取配置文件
        try:
            # 假设 sites.yaml 在项目根目录
            file_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'sites.yaml')
            print(f"========== [INIT] Reading config from: {file_path} ==========")
            
            with open(file_path, 'r', encoding='utf-8') as f:
                self.project_configs = yaml.safe_load(f)
                
                if not self.project_configs:
                    print("========== [ERROR] sites.yaml is empty! ==========")
                    return

                for site in self.project_configs:
                    url = site['url']
                    self.start_urls.append(url)
                    
                    # 提取域名加入 allowed_domains
                    # 例如 https://cs.ustc.edu.cn/ -> cs.ustc.edu.cn
                    domain = urlparse(url).netloc
                    self.allowed_domains.append(domain)
                    print(f"========== [INIT] Added Seed: {url} | Domain: {domain} ==========")
                    
        except FileNotFoundError:
            print(f"========== [FATAL] sites.yaml not found at {file_path} ==========")
        except Exception as e:
            print(f"========== [FATAL] Error loading config: {e} ==========")

    def parse_item(self, response):
        # 1. 确定当前页面属于哪个学院 (Project)
        project_name = 'unknown'
        current_domain = urlparse(response.url).netloc
        
        for config in self.project_configs:
            # 如果配置里的域名出现在当前 URL 的域名中
            config_domain = urlparse(config['url']).netloc
            if config_domain in current_domain:
                project_name = config['name']
                break
        
        # 2. 提取基础信息
        item = GeneralSpiderItem()
        item['url'] = response.url
        item['project'] = project_name
        
        # 提取标题 (优先取 title 标签，取不到就为空)
        item['title'] = response.xpath('//title/text()').get(default='').strip()
        
        # 3. 提取正文 (简单的清洗逻辑)
        # 提取所有 p 标签和 div 标签的文本，去除空白
        text_nodes = response.xpath('//body//text()').getall()
        clean_text = ''.join([t.strip() for t in text_nodes if t.strip()])
        item['parsed_text'] = clean_text[:50000] # 限制长度防止溢出
        
        # 保存原始 HTML (用于后续容错)
        # item['html_content'] = response.text 
        # 考虑到 HBase 存储压力，暂不存原始 HTML，如果需要可取消注释
        item['html_content'] = "" 

        # 4. 提取附件链接
        # 寻找所有 href 结尾是 pdf/doc/docx/xls/xlsx 的链接
        file_urls = []
        links = response.css('a::attr(href)').getall()
        for link in links:
            lower_link = link.lower()
            if lower_link.endswith(('.pdf', '.doc', '.docx', '.xls', '.xlsx', '.zip')):
                # 将相对链接转换为绝对链接
                abs_url = response.urljoin(link)
                file_urls.append(abs_url)
        
        item['file_urls'] = file_urls
        
        # 5. 只有当有内容或者有文件时才 yield
        if item['title'] or item['file_urls']:
            print(f"[Scraper] Found: {item['title']} - Files: {len(file_urls)}")
            yield item