import scrapy

class GeneralSpiderItem(scrapy.Item):  # <--- 名字必须是 GeneralSpiderItem
    # 基础信息
    url = scrapy.Field()
    project = scrapy.Field()
    title = scrapy.Field()
    
    # 内容
    html_content = scrapy.Field()
    parsed_text = scrapy.Field()
    date = scrapy.Field() # 可选，发布时间
    
    # 文件相关 (给 FilesPipeline 用的)
    file_urls = scrapy.Field() # 必须叫 file_urls
    files = scrapy.Field()     # 必须叫 files