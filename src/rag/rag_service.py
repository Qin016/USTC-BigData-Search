from langchain_ollama import OllamaLLM
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from search_engine import USTCSearchEngine
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)

class RAGService:
    def __init__(self):
        # 初始化 LLM，连接本地 Ollama
        # 确保本地已运行 `ollama run qwen2.5:7b`
        self.llm = OllamaLLM(model="qwen2.5:7b")
        
        # 初始化搜索引擎
        self.search_engine = USTCSearchEngine()
        
        # 定义 Prompt 模板
        # 要求模型作为“中科大文件搜索助手”，仅根据参考资料回答
        self.prompt_template = ChatPromptTemplate.from_template("""
你是一个智能助手“中科大文件搜索助手”，专门帮助用户查找和理解中国科学技术大学（USTC）的相关文件和资料。
请严格基于以下提供的参考资料（Context）回答用户的问题。这些资料来源于学校网站的附件文档或包含附件的网页。
如果参考资料中没有相关信息，请礼貌地告知用户你无法从当前资料中找到答案，不要编造信息。

参考资料：
{context}

用户问题：
{question}

回答：
""")
        
        # 构建 Chain
        self.chain = self.prompt_template | self.llm | StrOutputParser()

    def get_answer_stream(self, query):
        """
        获取 RAG 回答（流式）
        :param query: 用户问题
        :return: (generator, search_results)
        """
        # 1. 检索相关文档
        logging.info(f"Searching for: {query}")
        # 拉取全部候选供前端分页，LLM 只取前 10 条作为上下文
        search_results = self.search_engine.search(query, top_k=None)
        context_results = search_results[:10]
        
        # 2. 构建 Context
        if not context_results:
            context = "没有找到相关参考资料。"
        else:
            context_parts = []
            for i, res in enumerate(context_results):
                # 使用 snippet 作为上下文内容
                content_snippet = res.get('snippet', '')
                
                # 构建来源信息
                if res.get('type') == 'file':
                    source_info = f"类型: 文档 | 来源页面: {res.get('parent_url', '未知')}"
                else:
                    files_count = len(res.get('file_paths', []))
                    source_info = f"类型: 网页 (含 {files_count} 个附件) | URL: {res['url']}"
                
                context_parts.append(f"【资料 {i+1}】\n标题: {res['title']}\n{source_info}\n内容摘要: {content_snippet}")
            context = "\n\n".join(context_parts)
            
        logging.info("Context constructed. Generating answer...")

        # 3. 调用 LLM 生成回答 (流式)
        # stream 方法返回一个生成器，逐步输出 token
        stream_generator = self.chain.stream({
            "context": context,
            "question": query
        })
        
        return stream_generator, search_results

    def close(self):
        self.search_engine.close()
