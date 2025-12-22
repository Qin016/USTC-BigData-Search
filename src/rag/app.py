from flask import Flask, render_template, request, jsonify, Response, stream_with_context, send_from_directory
from rag_service import RAGService
import json
import logging
import os

app = Flask(__name__)

# 配置下载目录 (假设在 src/ustc_spider/downloads)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_FOLDER = os.path.join(BASE_DIR, '../ustc_spider/downloads')

# 初始化 RAG 服务
rag_service = RAGService()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/file/<path:filename>')
def serve_file(filename):
    """提供文件预览 (浏览器默认行为，如PDF会打开)"""
    return send_from_directory(DOWNLOAD_FOLDER, filename)

@app.route('/download/<path:filename>')
def download_file(filename):
    """强制下载文件"""
    return send_from_directory(DOWNLOAD_FOLDER, filename, as_attachment=True)

@app.route('/api/search', methods=['GET'])
def search():
    """
    搜索接口，使用 Server-Sent Events (SSE) 实现流式响应
    """
    query = request.args.get('q', '')
    if not query:
        return jsonify({'error': 'Query is required'}), 400

    try:
        # 获取流式生成器和搜索结果
        stream_gen, results = rag_service.get_answer_stream(query)
        
        # 准备搜索结果数据 (去掉全文 content 以减小传输量)
        simple_results = []
        for res in results:
            simple_results.append({
                'title': res['title'],
                'url': res['url'],
                'keywords': res.get('keywords', []),
                'file_paths': res.get('file_paths', []),
                'summary': res['summary'],
                'score': res['score']
            })

        def generate():
            # 1. 首先发送搜索结果元数据
            # event: results
            yield f"event: results\ndata: {json.dumps(simple_results, ensure_ascii=False)}\n\n"
            
            # 2. 发送 AI 回答的流式 Token
            # event: token
            for chunk in stream_gen:
                if chunk:
                    # 使用 json.dumps 确保特殊字符被正确转义
                    yield f"event: token\ndata: {json.dumps(chunk, ensure_ascii=False)}\n\n"
            
            # 3. 结束信号
            yield "event: done\ndata: [DONE]\n\n"

        return Response(stream_with_context(generate()), mimetype='text/event-stream')

    except Exception as e:
        logging.error(f"Search error: {e}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
