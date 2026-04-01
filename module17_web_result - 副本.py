import json
import threading
import socket
import webbrowser
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse
import html


class ResultViewerServer:
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if hasattr(self, '_initialized') and self._initialized:
            return
        self._initialized = True
        self.server = None
        self.server_thread = None
        self.port = None
        self.results_data = {}
        self.current_result_id = 0
    
    def _find_free_port(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('', 0))
            return s.getsockname()[1]
    
    def start_server(self):
        if self.server is not None:
            return self.port
        
        self.port = self._find_free_port()
        
        handler = ResultViewerHandler
        handler.server_instance = self
        
        self.server = HTTPServer(('127.0.0.1', self.port), handler)
        self.server_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.server_thread.start()
        
        return self.port
    
    def stop_server(self):
        if self.server:
            self.server.shutdown()
            self.server = None
            self.server_thread = None
    
    def add_result(self, columns, rows, query_info=None):
        self.current_result_id += 1
        result_id = self.current_result_id
        
        self.results_data[result_id] = {
            'columns': columns,
            'rows': rows,
            'query_info': query_info or {},
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }
        
        return result_id
    
    def get_result(self, result_id):
        return self.results_data.get(result_id)
    
    def open_browser(self, result_id):
        if self.port is None:
            self.start_server()
        
        url = f'http://127.0.0.1:{self.port}/result/{result_id}'
        webbrowser.open(url)
        return url


class ResultViewerHandler(BaseHTTPRequestHandler):
    server_instance = None
    
    def log_message(self, format, *args):
        pass
    
    def do_GET(self):
        parsed_path = urlparse(self.path)
        
        if parsed_path.path.startswith('/result/'):
            try:
                result_id = int(parsed_path.path.split('/')[-1])
                self._serve_result_page(result_id)
            except (ValueError, IndexError):
                self._serve_error_page("无效的结果ID")
        elif parsed_path.path == '/api/data':
            self._serve_api_data(parsed_path)
        elif parsed_path.path == '/' or parsed_path.path == '':
            self._serve_index_page()
        else:
            self._serve_error_page("页面未找到")
    
    def do_POST(self):
        parsed_path = urlparse(self.path)
        
        if parsed_path.path == '/api/copy':
            self._handle_copy_request()
        else:
            self.send_error(404)
    
    def _serve_result_page(self, result_id):
        result = self.server_instance.get_result(result_id)
        
        if result is None:
            self._serve_error_page("结果不存在或已过期")
            return
        
        html_content = self._generate_result_html(result_id, result)
        
        self.send_response(200)
        self.send_header('Content-type', 'text/html; charset=utf-8')
        self.end_headers()
        self.wfile.write(html_content.encode('utf-8'))
    
    def _serve_api_data(self, parsed_path):
        query_params = parse_qs(parsed_path.query)
        result_id = int(query_params.get('id', [0])[0])
        
        result = self.server_instance.get_result(result_id)
        
        if result is None:
            self.send_response(404)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'error': 'Result not found'}).encode('utf-8'))
            return
        
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(result).encode('utf-8'))
    
    def _serve_index_page(self):
        html_content = '''<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>SQL查询结果</title>
    <style>
        body { font-family: 'Segoe UI', 'Microsoft YaHei UI', Arial, sans-serif; margin: 0; background: #2d2d2d; color: #ccc; }
        .container { max-width: 1200px; margin: 0 auto; background: #3c3c3c; padding: 20px; min-height: 100vh; }
        h1 { color: #fff; margin-bottom: 20px; font-size: 16px; }
        .info { color: #888; margin-bottom: 20px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>SQL查询结果查看器</h1>
        <div class="info">请从应用程序中执行查询以查看结果</div>
    </div>
</body>
</html>'''
        
        self.send_response(200)
        self.send_header('Content-type', 'text/html; charset=utf-8')
        self.end_headers()
        self.wfile.write(html_content.encode('utf-8'))
    
    def _serve_error_page(self, message):
        html_content = f'''<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>错误</title>
    <style>
        body {{ font-family: 'Segoe UI', 'Microsoft YaHei UI', Arial, sans-serif; margin: 0; background: #2d2d2d; color: #ccc; }}
        .container {{ max-width: 600px; margin: 0 auto; background: #3c3c3c; padding: 20px; min-height: 100vh; }}
        .error {{ color: #f48771; padding: 20px; background: #4a1f1f; border-radius: 4px; border-left: 3px solid #f48771; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="error">{html.escape(message)}</div>
    </div>
</body>
</html>'''
        
        self.send_response(404)
        self.send_header('Content-type', 'text/html; charset=utf-8')
        self.end_headers()
        self.wfile.write(html_content.encode('utf-8'))
    
    def _handle_copy_request(self):
        content_length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_length)
        
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({'status': 'ok'}).encode('utf-8'))
    
    def _generate_result_html(self, result_id, result):
        columns_info = result.get('columns', [])
        rows = result.get('rows', [])
        query_info = result.get('query_info', {})
        timestamp = result.get('timestamp', '')
        elapsed_time = query_info.get('elapsed_time', 0)
        database = query_info.get('database', '')
        
        if columns_info and isinstance(columns_info[0], dict):
            column_names = [col.get('name', '') for col in columns_info]
            column_types = {col.get('name', ''): col.get('type', '') for col in columns_info}
            column_tables = {col.get('name', ''): col.get('table', '') for col in columns_info}
        else:
            column_names = columns_info if columns_info else []
            column_types = query_info.get('column_types', {})
            column_tables = {}
        
        columns_json = json.dumps(column_names)
        rows_json = json.dumps(rows)
        column_types_json = json.dumps(column_types)
        
        return f'''<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>SQL查询结果</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: 'Segoe UI', 'Microsoft YaHei UI', Arial, sans-serif;
            background: #f5f5f5;
            color: #333;
            font-size: 12px;
            overflow: hidden;
            height: 100vh;
            display: flex;
            flex-direction: column;
        }}
        
        .main-container {{
            display: flex;
            flex: 1;
            overflow: hidden;
        }}
        
        .sidebar {{
            width: 220px;
            background: #fff;
            border-right: 1px solid #ddd;
            display: flex;
            flex-direction: column;
            flex-shrink: 0;
        }}
        
        .sidebar-header {{
            padding: 8px 12px;
            background: #f5f5f5;
            border-bottom: 1px solid #ddd;
            font-weight: 600;
            color: #333;
            font-size: 12px;
        }}
        
        .sidebar-search {{
            padding: 6px 8px;
            border-bottom: 1px solid #eee;
        }}
        
        .sidebar-search input {{
            width: 100%;
            padding: 5px 8px;
            border: 1px solid #ddd;
            border-radius: 3px;
            font-size: 11px;
            background: #fff;
            color: #333;
        }}
        
        .sidebar-search input:focus {{
            outline: none;
            border-color: #4a90d9;
        }}
        
        .column-list {{
            flex: 1;
            overflow-y: auto;
            padding: 0;
        }}
        
        .column-item {{
            padding: 6px 10px;
            cursor: pointer;
            display: flex;
            align-items: center;
            gap: 6px;
            border-left: 3px solid transparent;
            font-size: 12px;
        }}
        
        .column-item:hover {{
            background: #e8f4fc;
            border-left-color: #4a90d9;
        }}
        
        .column-item.selected {{
            background: #d0e8f8;
            border-left-color: #4a90d9;
        }}
        
        .column-icon {{
            width: 14px;
            height: 14px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 9px;
            color: #4a90d9;
            background: #e8f4fc;
            border-radius: 2px;
        }}
        
        .column-name {{
            flex: 1;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
            color: #333;
            font-weight: 500;
        }}
        
        .column-type {{
            font-size: 10px;
            color: #888;
            background: #f5f5f5;
            padding: 1px 4px;
            border-radius: 2px;
        }}
        
        .table-group {{
            margin-bottom: 2px;
        }}
        
        .table-group-header {{
            padding: 8px 10px;
            background: #e8e8e8;
            cursor: pointer;
            display: flex;
            align-items: center;
            gap: 8px;
            font-weight: 600;
            color: #333;
            border-bottom: 1px solid #ddd;
        }}
        
        .table-group-header:hover {{
            background: #ddd;
        }}
        
        .toggle-icon {{
            font-size: 10px;
            transition: transform 0.2s;
        }}
        
        .table-group.collapsed .toggle-icon {{
            transform: rotate(-90deg);
        }}
        
        .table-name {{
            flex: 1;
        }}
        
        .column-count {{
            font-size: 11px;
            color: #888;
            font-weight: normal;
        }}
        
        .table-columns {{
            display: block;
        }}
        
        .table-group.collapsed .table-columns {{
            display: none;
        }}
        
        .content-area {{
            flex: 1;
            display: flex;
            flex-direction: column;
            overflow: hidden;
        }}
        
        .toolbar {{
            background: #f8f8f8;
            border-bottom: 1px solid #ddd;
            padding: 6px 8px;
            display: flex;
            align-items: center;
            gap: 4px;
            flex-shrink: 0;
        }}
        
        .toolbar-btn {{
            background: #fff;
            border: 1px solid #ccc;
            padding: 4px 8px;
            border-radius: 3px;
            cursor: pointer;
            font-size: 11px;
            color: #333;
            display: flex;
            align-items: center;
            gap: 4px;
            transition: all 0.15s;
        }}
        
        .toolbar-btn:hover {{
            background: #e8e8e8;
            border-color: #aaa;
        }}
        
        .toolbar-btn:active {{
            background: #d8d8d8;
        }}
        
        .toolbar-separator {{
            width: 1px;
            height: 18px;
            background: #ddd;
            margin: 0 4px;
        }}
        
        .tabs-container {{
            background: #f5f5f5;
            border-bottom: 1px solid #ddd;
            display: flex;
            align-items: center;
            padding: 0 8px;
            flex-shrink: 0;
        }}
        
        .tab {{
            padding: 8px 16px;
            background: transparent;
            border: none;
            color: #666;
            cursor: pointer;
            font-size: 12px;
            border-bottom: 2px solid transparent;
            transition: all 0.15s;
        }}
        
        .tab:hover {{
            color: #333;
            background: #e8e8e8;
        }}
        
        .tab.active {{
            color: #333;
            border-bottom-color: #4a90d9;
            background: #fff;
        }}
        
        .tab-info {{
            margin-left: auto;
            color: #666;
            font-size: 11px;
            padding: 0 12px;
        }}
        
        .table-container {{
            flex: 1;
            overflow: auto;
            background: #fff;
        }}
        
        table {{
            border-collapse: collapse;
            width: 100%;
            min-width: max-content;
        }}
        
        th {{
            background: linear-gradient(to bottom, #f5f5f5 0%, #e8e8e8 100%);
            border: 1px solid #ccc;
            padding: 8px 12px;
            text-align: left;
            font-weight: 600;
            color: #222;
            position: sticky;
            top: 0;
            z-index: 10;
            white-space: nowrap;
            user-select: none;
            cursor: pointer;
            font-size: 12px;
            min-width: 80px;
        }}
        
        th:hover {{
            background: linear-gradient(to bottom, #e8e8e8 0%, #d8d8d8 100%);
        }}
        
        th.resizing {{
            cursor: col-resize;
        }}
        
        .resize-handle {{
            position: absolute;
            right: 0;
            top: 0;
            bottom: 0;
            width: 5px;
            cursor: col-resize;
            background: transparent;
        }}
        
        .resize-handle:hover {{
            background: #4a90d9;
        }}
        
        th.row-num {{
            width: 50px;
            text-align: center;
            background: linear-gradient(to bottom, #f0f0f0 0%, #e0e0e0 100%);
            color: #666;
        }}
        
        th.sorted-asc::after {{
            content: ' ▲';
            font-size: 9px;
            color: #4a90d9;
        }}
        
        th.sorted-desc::after {{
            content: ' ▼';
            font-size: 9px;
            color: #4a90d9;
        }}
        
        td {{
            border: 1px solid #e0e0e0;
            padding: 5px 10px;
            font-family: 'Consolas', 'Monaco', 'Courier New', monospace;
            font-size: 12px;
            white-space: pre;
            max-width: 400px;
            overflow: hidden;
            text-overflow: ellipsis;
            color: #222;
        }}
        
        td.row-num {{
            text-align: center;
            color: #666;
            background: #fafafa;
            font-family: 'Segoe UI', Arial, sans-serif;
            font-size: 11px;
            min-width: 50px;
        }}
        
        tr:nth-child(even) td:not(.row-num) {{
            background: #fafafa;
        }}
        
        tr:nth-child(odd) td:not(.row-num) {{
            background: #fff;
        }}
        
        tr:hover td {{
            background: #e3f2fd !important;
        }}
        
        tr.selected td {{
            background: #bbdefb !important;
        }}
        
        td.null-value {{
            color: #999;
            font-style: italic;
        }}
        
        td.number {{
            text-align: right;
            color: #0055aa;
            font-weight: 500;
        }}
        
        td.string {{
            color: #222;
        }}
        
        td.datetime {{
            color: #007700;
        }}
        
        td.json {{
            color: #005588;
            cursor: pointer;
        }}
        
        td.html {{
            color: #555588;
            cursor: pointer;
        }}
        
        td.xml {{
            color: #885500;
            cursor: pointer;
        }}
        
        td.image {{
            color: #aa5500;
            cursor: pointer;
        }}
        
        td.longtext {{
            color: #222;
            cursor: pointer;
        }}
        
        .status-bar {{
            background: #4a90d9;
            padding: 3px 12px;
            font-size: 11px;
            color: #fff;
            display: flex;
            align-items: center;
            gap: 20px;
            flex-shrink: 0;
        }}
        
        .status-item {{
            display: flex;
            align-items: center;
            gap: 4px;
        }}
        
        .status-item .icon {{
            font-size: 12px;
        }}
        
        .context-menu {{
            position: fixed;
            background: #fff;
            border: 1px solid #ccc;
            border-radius: 4px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
            z-index: 1000;
            display: none;
            min-width: 180px;
            padding: 4px 0;
        }}
        
        .context-menu-item {{
            padding: 6px 16px;
            cursor: pointer;
            font-size: 12px;
            display: flex;
            align-items: center;
            gap: 8px;
        }}
        
        .context-menu-item:hover {{
            background: #4a90d9;
            color: #fff;
        }}
        
        .context-menu-item .icon {{
            width: 16px;
            text-align: center;
        }}
        
        .context-menu-separator {{
            height: 1px;
            background: #eee;
            margin: 4px 0;
        }}
        
        .search-box {{
            display: flex;
            align-items: center;
            background: #fff;
            border: 1px solid #ccc;
            border-radius: 3px;
            padding: 0 6px;
            margin-left: auto;
        }}
        
        .search-box input {{
            background: transparent;
            border: none;
            padding: 4px;
            font-size: 11px;
            width: 150px;
            color: #333;
            outline: none;
        }}
        
        .search-box input::placeholder {{
            color: #999;
        }}
        
        .highlight {{
            background: #fff59d;
            color: #333;
            padding: 0 2px;
            border-radius: 2px;
        }}
        
        .no-data {{
            text-align: center;
            padding: 40px;
            color: #666;
            font-style: italic;
        }}
        
        .filter-header {{
            background: #f8f8f8 !important;
            padding: 4px 6px !important;
            border: 1px solid #ddd !important;
        }}
        
        .filter-input {{
            width: 100%;
            background: #fff;
            border: 1px solid #ccc;
            padding: 3px 6px;
            font-size: 11px;
            color: #333;
            border-radius: 2px;
            box-sizing: border-box;
        }}
        
        .filter-input:focus {{
            outline: none;
            border-color: #4a90d9;
        }}
        
        .clear-filter-btn {{
            background: #f44336;
            color: #fff;
            border: none;
            border-radius: 3px;
            width: 22px;
            height: 22px;
            cursor: pointer;
            font-size: 12px;
            display: flex;
            align-items: center;
            justify-content: center;
        }}
        
        .clear-filter-btn:hover {{
            background: #d32f2f;
        }}
        
        .cell-editor {{
            position: fixed;
            background: #1e1e1e;
            border: 1px solid #4a90d9;
            padding: 4px;
            z-index: 100;
            display: none;
        }}
        
        .cell-editor textarea {{
            background: transparent;
            border: none;
            color: #333;
            font-family: 'Consolas', monospace;
            font-size: 12px;
            resize: none;
            outline: none;
            min-width: 200px;
            min-height: 60px;
        }}
        
        .modal {{
            display: none;
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: rgba(0, 0, 0, 0.5);
            z-index: 2000;
            justify-content: center;
            align-items: center;
        }}
        
        .modal.active {{
            display: flex;
        }}
        
        .modal-content {{
            background: #fff;
            border: 1px solid #ddd;
            border-radius: 8px;
            max-width: 90%;
            max-height: 90%;
            overflow: auto;
            position: relative;
            box-shadow: 0 4px 20px rgba(0,0,0,0.2);
        }}
        
        .modal-header {{
            padding: 12px 16px;
            background: #f5f5f5;
            border-bottom: 1px solid #ddd;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        
        .modal-title {{
            font-weight: 600;
            color: #333;
            font-size: 13px;
        }}
        
        .modal-close {{
            background: transparent;
            border: none;
            color: #999;
            font-size: 20px;
            cursor: pointer;
            padding: 4px;
            line-height: 1;
        }}
        
        .modal-close:hover {{
            color: #f44336;
        }}
        
        .modal-body {{
            padding: 16px;
            overflow: auto;
            max-height: calc(90vh - 120px);
        }}
        
        .modal-body img {{
            max-width: 100%;
            max-height: 70vh;
            object-fit: contain;
        }}
        
        .modal-body pre {{
            background: #f8f8f8;
            padding: 12px;
            border-radius: 4px;
            border: 1px solid #eee;
            overflow: auto;
            font-family: 'Consolas', monospace;
            font-size: 12px;
            color: #333;
            white-space: pre-wrap;
            margin: 0;
        }}
        
        .value-actions {{
            display: flex;
            gap: 8px;
            padding: 8px;
            background: #f5f5f5;
            border-top: 1px solid #ddd;
        }}
        
        .value-action-btn {{
            background: #4a4a4a;
            border: 1px solid #555;
            color: #ccc;
            padding: 4px 12px;
            border-radius: 3px;
            cursor: pointer;
            font-size: 11px;
        }}
        
        .value-action-btn:hover {{
            background: #555;
            border-color: #4a90d9;
        }}
        
        ::-webkit-scrollbar {{
            width: 10px;
            height: 10px;
        }}
        
        ::-webkit-scrollbar-track {{
            background: #1e1e1e;
        }}
        
        ::-webkit-scrollbar-thumb {{
            background: #4a4a4a;
            border-radius: 5px;
        }}
        
        ::-webkit-scrollbar-thumb:hover {{
            background: #555;
        }}
        
        ::-webkit-scrollbar-corner {{
            background: #1e1e1e;
        }}
    </style>
</head>
<body>
    <div class="main-container">
        <div class="sidebar">
            <div class="sidebar-header">列信息 ({len(column_names)})</div>
            <div class="sidebar-search">
                <input type="text" id="columnSearch" placeholder="搜索列名..." oninput="filterColumns()">
            </div>
            <div class="column-list" id="columnList">
                {self._generate_column_list(columns_info, column_types, column_tables)}
            </div>
        </div>
        
        <div class="content-area">
            <div class="toolbar">
                <button class="toolbar-btn" onclick="refreshData()" title="刷新">
                    <span>🔄</span> 刷新
                </button>
                <div class="toolbar-separator"></div>
                <button class="toolbar-btn" onclick="copySelectedRows()" title="复制选中行">
                    <span>📋</span> 复制
                </button>
                <button class="toolbar-btn" onclick="copyAllRows()" title="复制全部">
                    <span>📄</span> 全部复制
                </button>
                <button class="toolbar-btn" onclick="copyAsInsert()" title="复制为INSERT语句">
                    <span>📝</span> INSERT
                </button>
                <div class="toolbar-separator"></div>
                <button class="toolbar-btn" onclick="exportCSV()" title="导出CSV">
                    <span>💾</span> CSV
                </button>
                <button class="toolbar-btn" onclick="exportJSON()" title="导出JSON">
                    <span>📊</span> JSON
                </button>
                <div class="toolbar-separator"></div>
                <button class="toolbar-btn" onclick="toggleFilter()" title="筛选">
                    <span>🔍</span> 筛选
                </button>
                <div class="search-box">
                    <span>🔍</span>
                    <input type="text" id="searchInput" placeholder="搜索..." oninput="searchTable()">
                </div>
            </div>
            
            <div class="tabs-container">
                <div class="tab active">结果集 1</div>
                <div class="tab-info">
                    {database} | {timestamp} | 耗时: {elapsed_time:.2f}s
                </div>
            </div>
            
            <div class="table-container" id="tableContainer">
                <table id="resultTable">
                    <thead>
                        <tr>
                            <th class="row-num">#</th>
                            {self._generate_table_headers(column_names)}
                        </tr>
                        <tr id="filterRow" style="display:none;">
                            {self._generate_filter_row(column_names)}
                        </tr>
                    </thead>
                    <tbody id="tableBody">
                        {self._generate_table_rows(column_names, rows, column_types)}
                    </tbody>
                </table>
            </div>
        </div>
    </div>
    
    <div class="status-bar">
        <div class="status-item">
            <span class="icon">📊</span>
            <span>行数: <strong id="rowCount">{len(rows)}</strong></span>
        </div>
        <div class="status-item">
            <span class="icon">📋</span>
            <span>列数: <strong>{len(column_names)}</strong></span>
        </div>
        <div class="status-item">
            <span class="icon">✓</span>
            <span>选中: <strong id="selectedCount">0</strong></span>
        </div>
        <div class="status-item" style="margin-left: auto;">
            <span id="statusText">就绪</span>
        </div>
    </div>
    
    <div class="context-menu" id="contextMenu">
        <div class="context-menu-item" onclick="copyCell()">
            <span class="icon">📋</span> 复制单元格
        </div>
        <div class="context-menu-item" onclick="copyRow()">
            <span class="icon">📄</span> 复制整行
        </div>
        <div class="context-menu-item" onclick="copyColumn()">
            <span class="icon">📑</span> 复制整列
        </div>
        <div class="context-menu-separator"></div>
        <div class="context-menu-item" onclick="filterByValue()">
            <span class="icon">🔍</span> 按此值筛选
        </div>
        <div class="context-menu-item" onclick="setNullFilter()">
            <span class="icon">🚫</span> 筛选NULL值
        </div>
        <div class="context-menu-separator"></div>
        <div class="context-menu-item" onclick="selectAll()">
            <span class="icon">☑</span> 全选
        </div>
        <div class="context-menu-item" onclick="deselectAll()">
            <span class="icon">☐</span> 取消全选
        </div>
        <div class="context-menu-separator"></div>
        <div class="context-menu-item" onclick="copyAsInsert()">
            <span class="icon">📝</span> 复制为INSERT
        </div>
        <div class="context-menu-item" onclick="copyAsUpdate()">
            <span class="icon">✏️</span> 复制为UPDATE
        </div>
    </div>
    
    <div class="context-menu" id="columnContextMenu">
        <div class="context-menu-item" onclick="copyColumnName()">
            <span class="icon">📋</span> 复制列名
        </div>
        <div class="context-menu-item" onclick="copyColumnValues()">
            <span class="icon">📄</span> 复制所有值
        </div>
        <div class="context-menu-separator"></div>
        <div class="context-menu-item" onclick="document.getElementById('columnContextMenu').style.display='none';">
            <span class="icon">❌</span> 关闭
        </div>
    </div>
    
    <div class="modal" id="valueModal">
        <div class="modal-content">
            <div class="modal-header">
                <div class="modal-title" id="modalTitle">查看值</div>
                <button class="modal-close" onclick="closeModal()">&times;</button>
            </div>
            <div class="modal-body" id="modalBody"></div>
            <div class="value-actions">
                <button class="value-action-btn" onclick="copyModalValue()">复制</button>
                <button class="value-action-btn" onclick="formatJson()">格式化JSON</button>
                <button class="value-action-btn" onclick="downloadValue()">下载</button>
            </div>
        </div>
    </div>
    
    <script>
        const columns = {columns_json};
        const columnTypes = {column_types_json};
        const columnTables = {json.dumps(column_tables)};
        const rows = {rows_json};
        let selectedRows = new Set();
        let currentSortColumn = -1;
        let sortDirection = 'asc';
        let contextMenuTarget = null;
        let filterValues = {{}};
        let currentModalValue = null;
        
        document.addEventListener('click', function(e) {{
            if (!e.target.closest('.context-menu')) {{
                document.getElementById('contextMenu').style.display = 'none';
            }}
        }});
        
        document.addEventListener('contextmenu', function(e) {{
            if (e.target.tagName === 'TD' || e.target.tagName === 'TH') {{
                e.preventDefault();
                contextMenuTarget = e.target;
                const menu = document.getElementById('contextMenu');
                menu.style.display = 'block';
                
                let left = e.pageX;
                let top = e.pageY;
                
                if (left + menu.offsetWidth > window.innerWidth) {{
                    left = window.innerWidth - menu.offsetWidth - 10;
                }}
                if (top + menu.offsetHeight > window.innerHeight) {{
                    top = window.innerHeight - menu.offsetHeight - 10;
                }}
                
                menu.style.left = left + 'px';
                menu.style.top = top + 'px';
            }}
        }});
        
        document.addEventListener('dblclick', function(e) {{
            if (e.target.tagName === 'TD' && !e.target.classList.contains('row-num')) {{
                const valueType = e.target.dataset.valueType || 'text';
                const colIndex = e.target.cellIndex - 1;
                const colName = columns[colIndex];
                const colType = columnTypes[colName] || '';
                
                if (colType === 'BLOB' || colType === 'TINYBLOB' || colType === 'MEDIUMBLOB' || colType === 'LONGBLOB') {{
                    const rawValue = e.target.dataset.rawValue;
                    if (rawValue && rawValue.startsWith('data:')) {{
                        showImageModal(rawValue);
                    }} else {{
                        showStatus('无法显示此BLOB数据');
                    }}
                }} else if (valueType === 'longtext' || valueType === 'json' || valueType === 'image') {{
                    const value = e.target.dataset.rawValue || e.target.textContent.trim();
                    showValueModal(value, valueType);
                }} else {{
                    const value = e.target.dataset.rawValue || e.target.textContent.trim();
                    if (value && value.length > 200) {{
                        showValueModal(value, 'text');
                    }}
                }}
            }}
        }});
        
        document.addEventListener('keydown', function(e) {{
            if (e.ctrlKey && e.key === 'c') {{
                if (selectedRows.size > 0) {{
                    copySelectedRows();
                }} else {{
                    copyAllRows();
                }}
                e.preventDefault();
            }}
            if (e.ctrlKey && e.key === 'a') {{
                selectAll();
                e.preventDefault();
            }}
        }});
        
        function toggleRowSelection(row, event) {{
            if (event && event.ctrlKey) {{
                const rowIndex = parseInt(row.dataset.rowIndex);
                if (selectedRows.has(rowIndex)) {{
                    selectedRows.delete(rowIndex);
                    row.classList.remove('selected');
                }} else {{
                    selectedRows.add(rowIndex);
                    row.classList.add('selected');
                }}
            }} else if (event && event.shiftKey && selectedRows.size > 0) {{
                const lastSelected = Array.from(selectedRows).pop();
                const currentRow = parseInt(row.dataset.rowIndex);
                const start = Math.min(lastSelected, currentRow);
                const end = Math.max(lastSelected, currentRow);
                
                document.querySelectorAll('#tableBody tr').forEach(r => {{
                    const idx = parseInt(r.dataset.rowIndex);
                    if (idx >= start && idx <= end) {{
                        selectedRows.add(idx);
                        r.classList.add('selected');
                    }}
                }});
            }} else {{
                const rowIndex = parseInt(row.dataset.rowIndex);
                if (selectedRows.has(rowIndex) && selectedRows.size === 1) {{
                    selectedRows.delete(rowIndex);
                    row.classList.remove('selected');
                }} else {{
                    selectedRows.clear();
                    document.querySelectorAll('#tableBody tr').forEach(r => {{
                        r.classList.remove('selected');
                    }});
                    selectedRows.add(rowIndex);
                    row.classList.add('selected');
                }}
            }}
            updateSelectedCount();
        }}
        
        function updateSelectedCount() {{
            document.getElementById('selectedCount').textContent = selectedRows.size;
        }}
        
        function selectAll() {{
            document.querySelectorAll('#tableBody tr').forEach(row => {{
                selectedRows.add(parseInt(row.dataset.rowIndex));
                row.classList.add('selected');
            }});
            updateSelectedCount();
            hideContextMenu();
        }}
        
        function deselectAll() {{
            selectedRows.clear();
            document.querySelectorAll('#tableBody tr').forEach(row => {{
                row.classList.remove('selected');
            }});
            updateSelectedCount();
            hideContextMenu();
        }}
        
        function copyToClipboard(text) {{
            navigator.clipboard.writeText(text).catch(() => {{
                const textarea = document.createElement('textarea');
                textarea.value = text;
                document.body.appendChild(textarea);
                textarea.select();
                document.execCommand('copy');
                document.body.removeChild(textarea);
            }});
        }}
        
        function copyCell() {{
            if (contextMenuTarget) {{
                copyToClipboard(contextMenuTarget.textContent.trim());
                showStatus('已复制单元格内容');
            }}
            hideContextMenu();
        }}
        
        function copyRow() {{
            if (contextMenuTarget) {{
                const row = contextMenuTarget.closest('tr');
                const cells = Array.from(row.querySelectorAll('td')).slice(1);
                const text = cells.map(cell => cell.textContent.trim()).join('\\t');
                copyToClipboard(text);
                showStatus('已复制行内容');
            }}
            hideContextMenu();
        }}
        
        function copyColumn() {{
            if (contextMenuTarget) {{
                const cellIndex = contextMenuTarget.cellIndex;
                const values = [];
                document.querySelectorAll('#tableBody tr').forEach(row => {{
                    values.push(row.cells[cellIndex].textContent.trim());
                }});
                copyToClipboard(values.join('\\n'));
                showStatus('已复制列内容 (' + values.length + ' 行)');
            }}
            hideContextMenu();
        }}
        
        function filterByValue() {{
            if (contextMenuTarget) {{
                const value = contextMenuTarget.textContent.trim();
                document.getElementById('searchInput').value = value;
                searchTable();
                showStatus('已按值筛选');
            }}
            hideContextMenu();
        }}
        
        function setNullFilter() {{
            if (contextMenuTarget) {{
                const cellIndex = contextMenuTarget.cellIndex;
                document.getElementById('searchInput').value = 'NULL';
                searchTable();
                showStatus('已筛选NULL值');
            }}
            hideContextMenu();
        }}
        
        function hideContextMenu() {{
            document.getElementById('contextMenu').style.display = 'none';
        }}
        
        function copySelectedRows() {{
            if (selectedRows.size === 0) {{
                showStatus('请先选择要复制的行');
                return;
            }}
            
            const header = columns.join('\\t');
            const lines = [header];
            
            const sortedRows = Array.from(selectedRows).sort((a, b) => a - b);
            sortedRows.forEach(rowIndex => {{
                const row = document.querySelector(`tr[data-row-index="${{rowIndex}}"]`);
                if (row) {{
                    const cells = Array.from(row.querySelectorAll('td')).slice(1);
                    lines.push(cells.map(cell => cell.textContent.trim()).join('\\t'));
                }}
            }});
            
            copyToClipboard(lines.join('\\n'));
            showStatus('已复制 ' + selectedRows.size + ' 行');
        }}
        
        function copyAllRows() {{
            const header = columns.join('\\t');
            const lines = [header];
            
            document.querySelectorAll('#tableBody tr').forEach(row => {{
                const cells = Array.from(row.querySelectorAll('td')).slice(1);
                lines.push(cells.map(cell => cell.textContent.trim()).join('\\t'));
            }});
            
            copyToClipboard(lines.join('\\n'));
            showStatus('已复制全部 ' + (lines.length - 1) + ' 行');
        }}
        
        function copyAsInsert() {{
            if (rows.length === 0) return;
            
            let insertSql = '';
            const tableName = 'table_name';
            
            const rowsToCopy = selectedRows.size > 0 
                ? Array.from(selectedRows).sort((a, b) => a - b).map(i => rows[i])
                : rows;
            
            rowsToCopy.forEach(row => {{
                const values = row.map(val => {{
                    if (val === '' || val === 'NULL') return 'NULL';
                    return "'" + String(val).replace(/'/g, "''") + "'";
                }}).join(', ');
                
                insertSql += `INSERT INTO ${{tableName}} (${{columns.join(', ')}}) VALUES (${{values}});\\n`;
            }});
            
            copyToClipboard(insertSql);
            showStatus('已复制为INSERT语句');
            hideContextMenu();
        }}
        
        function copyAsUpdate() {{
            if (rows.length === 0) return;
            
            let updateSql = '';
            const tableName = 'table_name';
            
            const rowsToCopy = selectedRows.size > 0 
                ? Array.from(selectedRows).sort((a, b) => a - b).map(i => rows[i])
                : rows;
            
            rowsToCopy.forEach(row => {{
                const sets = columns.map((col, i) => {{
                    const val = row[i];
                    if (val === '' || val === 'NULL') return `${{col}} = NULL`;
                    return `${{col}} = '${{String(val).replace(/'/g, "''")}}'`;
                }}).join(', ');
                
                updateSql += `UPDATE ${{tableName}} SET ${{sets}} WHERE <condition>;\\n`;
            }});
            
            copyToClipboard(updateSql);
            showStatus('已复制为UPDATE语句');
            hideContextMenu();
        }}
        
        function filterByValue() {{
            if (contextMenuTarget) {{
                const value = contextMenuTarget.textContent.trim();
                document.getElementById('searchInput').value = value;
                searchTable();
                showStatus('已按值筛选');
            }}
            hideContextMenu();
        }}
        
        function setNullFilter() {{
            document.getElementById('searchInput').value = 'NULL';
            searchTable();
            showStatus('已筛选NULL值');
            hideContextMenu();
        }}
        
        function selectAll() {{
            document.querySelectorAll('#tableBody tr').forEach(row => {{
                selectedRows.add(parseInt(row.dataset.rowIndex));
                row.classList.add('selected');
            }});
            updateSelectedCount();
            hideContextMenu();
        }}
        
        function deselectAll() {{
            selectedRows.clear();
            document.querySelectorAll('#tableBody tr').forEach(row => {{
                row.classList.remove('selected');
            }});
            updateSelectedCount();
            hideContextMenu();
        }}
        
        function exportCSV() {{
            let csv = columns.map(c => '"' + c.replace(/"/g, '""') + '"').join(',') + '\\n';
            
            document.querySelectorAll('#tableBody tr').forEach(row => {{
                const cells = Array.from(row.querySelectorAll('td')).slice(1);
                csv += cells.map(cell => {{
                    const val = cell.textContent.trim();
                    if (val.includes(',') || val.includes('"') || val.includes('\\n') || val.includes('\\r')) {{
                        return '"' + val.replace(/"/g, '""') + '"';
                    }}
                    return val;
                }}).join(',') + '\\n';
            }});
            
            downloadFile(csv, 'result.csv', 'text/csv');
            showStatus('已导出CSV文件');
        }}
        
        function exportJSON() {{
            const data = [];
            document.querySelectorAll('#tableBody tr').forEach(row => {{
                const cells = Array.from(row.querySelectorAll('td')).slice(1);
                const obj = {{}};
                columns.forEach((col, i) => {{
                    let val = cells[i].textContent.trim();
                    if (val === 'NULL') val = null;
                    obj[col] = val;
                }});
                data.push(obj);
            }});
            
            downloadFile(JSON.stringify(data, null, 2), 'result.json', 'application/json');
            showStatus('已导出JSON文件');
        }}
        
        function downloadFile(content, filename, type) {{
            const blob = new Blob([content], {{ type: type + ';charset=utf-8' }});
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = filename;
            a.click();
            URL.revokeObjectURL(url);
        }}
        
        function searchTable() {{
            const searchTerm = document.getElementById('searchInput').value.toLowerCase();
            const rows = document.querySelectorAll('#tableBody tr');
            let visibleCount = 0;
            
            rows.forEach(row => {{
                const text = row.textContent.toLowerCase();
                if (text.includes(searchTerm)) {{
                    row.style.display = '';
                    visibleCount++;
                    
                    if (searchTerm) {{
                        const cells = row.querySelectorAll('td');
                        cells.forEach(cell => {{
                            const originalText = cell.textContent;
                            const lowerText = originalText.toLowerCase();
                            const index = lowerText.indexOf(searchTerm);
                            
                            if (index >= 0) {{
                                const before = originalText.substring(0, index);
                                const match = originalText.substring(index, index + searchTerm.length);
                                const after = originalText.substring(index + searchTerm.length);
                                cell.innerHTML = before + '<span class="highlight">' + match + '</span>' + after;
                            }}
                        }});
                    }} else {{
                        const cells = row.querySelectorAll('td');
                        cells.forEach(cell => {{
                            cell.innerHTML = cell.textContent;
                        }});
                    }}
                }} else {{
                    row.style.display = 'none';
                }}
            }});
            
            document.getElementById('rowCount').textContent = visibleCount;
            showStatus('显示 ' + visibleCount + ' 条匹配记录');
        }}
        
        function sortTable(columnIndex) {{
            const table = document.getElementById('resultTable');
            const tbody = document.getElementById('tableBody');
            const rows = Array.from(tbody.querySelectorAll('tr'));
            
            if (currentSortColumn === columnIndex) {{
                sortDirection = sortDirection === 'asc' ? 'desc' : 'asc';
            }} else {{
                sortDirection = 'asc';
                currentSortColumn = columnIndex;
            }}
            
            document.querySelectorAll('th').forEach(th => {{
                th.classList.remove('sorted-asc', 'sorted-desc');
            }});
            
            const header = table.querySelectorAll('th')[columnIndex];
            header.classList.add(sortDirection === 'asc' ? 'sorted-asc' : 'sorted-desc');
            
            rows.sort((a, b) => {{
                const aVal = a.cells[columnIndex].textContent.trim();
                const bVal = b.cells[columnIndex].textContent.trim();
                
                if (aVal === 'NULL' && bVal === 'NULL') return 0;
                if (aVal === 'NULL') return 1;
                if (bVal === 'NULL') return -1;
                
                const aNum = parseFloat(aVal);
                const bNum = parseFloat(bVal);
                
                if (!isNaN(aNum) && !isNaN(bNum)) {{
                    return sortDirection === 'asc' ? aNum - bNum : bNum - aNum;
                }}
                
                const aDate = Date.parse(aVal);
                const bDate = Date.parse(bVal);
                
                if (!isNaN(aDate) && !isNaN(bDate)) {{
                    return sortDirection === 'asc' ? aDate - bDate : bDate - aDate;
                }}
                
                return sortDirection === 'asc' 
                    ? aVal.localeCompare(bVal, 'zh-CN')
                    : bVal.localeCompare(aVal, 'zh-CN');
            }});
            
            rows.forEach(row => tbody.appendChild(row));
            showStatus('已按列 ' + columns[columnIndex - 1] + ' 排序');
        }}
        
        function toggleFilter() {{
            const filterRow = document.getElementById('filterRow');
            if (filterRow.style.display === 'none') {{
                filterRow.style.display = '';
            }} else {{
                filterRow.style.display = 'none';
            }}
        }}
        
        let columnFilters = {{}};
        
        function applyColumnFilter(colIdx, filterValue) {{
            columnFilters[colIdx] = filterValue.toLowerCase();
            
            const allRows = document.querySelectorAll('#tableBody tr');
            let visibleCount = 0;
            
            allRows.forEach(row => {{
                let show = true;
                const cells = row.querySelectorAll('td');
                
                for (const [idx, filter] of Object.entries(columnFilters)) {{
                    if (filter && filter !== '') {{
                        const cellIndex = parseInt(idx) + 1;
                        if (cellIndex < cells.length) {{
                            const cellValue = cells[cellIndex].textContent.toLowerCase();
                            if (!cellValue.includes(filter)) {{
                                show = false;
                                break;
                            }}
                        }}
                    }}
                }}
                
                if (show) {{
                    row.style.display = '';
                    visibleCount++;
                }} else {{
                    row.style.display = 'none';
                }}
            }});
            
            document.getElementById('rowCount').textContent = visibleCount;
            showStatus('筛选后显示 ' + visibleCount + ' 条记录');
        }}
        
        function clearFilters() {{
            columnFilters = {{}};
            const inputs = document.querySelectorAll('.filter-input');
            inputs.forEach(input => input.value = '');
            const allRows = document.querySelectorAll('#tableBody tr');
            allRows.forEach(row => row.style.display = '');
            document.getElementById('rowCount').textContent = allRows.length;
            showStatus('已清除筛选');
        }}
        
        function refreshData() {{
            showStatus('数据已刷新');
        }}
        
        function showStatus(text) {{
            document.getElementById('statusText').textContent = text;
        }}
        
        function highlightColumn(colIndex) {{
            document.querySelectorAll('td:nth-child(' + (colIndex + 2) + ')').forEach(cell => {{
                cell.style.background = '#fff3cd';
            }});
        }}
        
        function unhighlightColumn(colIndex) {{
            document.querySelectorAll('td:nth-child(' + (colIndex + 2) + ')').forEach(cell => {{
                cell.style.background = '';
            }});
        }}
        
        function showValueModal(value, type) {{
            currentModalValue = value;
            const modal = document.getElementById('valueModal');
            const modalTitle = document.getElementById('modalTitle');
            const modalBody = document.getElementById('modalBody');
            
            modalTitle.textContent = '查看值 (' + type + ')';
            
            if (type === 'image' || isImageUrl(value)) {{
                modalBody.innerHTML = '<img src="' + escapeHtml(value) + '" alt="图片" onerror="this.onerror=null;this.parentNode.innerHTML=\\'<div style=\\'color:#f48771;padding:20px;\\'>图片加载失败</div>\\'">';
            }} else if (type === 'json' || isJson(value)) {{
                try {{
                    const json = typeof value === 'string' ? JSON.parse(value) : value;
                    modalBody.innerHTML = '<pre>' + JSON.stringify(json, null, 2) + '</pre>';
                }} catch(e) {{
                    modalBody.innerHTML = '<pre>' + escapeHtml(value) + '</pre>';
                }}
            }} else if (type === 'html' || isHtml(value)) {{
                const iframe = document.createElement('iframe');
                iframe.style.cssText = 'width:100%;height:400px;border:none;background:#fff;';
                iframe.srcdoc = value;
                modalBody.innerHTML = '';
                modalBody.appendChild(iframe);
            }} else if (type === 'xml' || isXml(value)) {{
                modalBody.innerHTML = '<pre>' + escapeHtml(formatXml(value)) + '</pre>';
            }} else {{
                modalBody.innerHTML = '<pre>' + escapeHtml(value) + '</pre>';
            }}
            
            modal.classList.add('active');
        }}
        
        function closeModal() {{
            document.getElementById('valueModal').classList.remove('active');
        }}
        
        function escapeHtml(text) {{
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }}
        
        function isJson(str) {{
            if (typeof str !== 'string') return false;
            str = str.trim();
            if ((str.startsWith('{{') && str.endsWith('}}')) || (str.startsWith('[') && str.endsWith(']'))) {{
                try {{
                    JSON.parse(str);
                    return true;
                }} catch(e) {{
                    return false;
                }}
            }}
            return false;
        }}
        
        function isHtml(str) {{
            return typeof str === 'string' && str.trim().startsWith('<') && str.trim().endsWith('>');
        }}
        
        function isXml(str) {{
            return typeof str === 'string' && str.trim().startsWith('<?xml');
        }}
        
        function isImageUrl(str) {{
            if (typeof str !== 'string') return false;
            const lower = str.toLowerCase();
            return lower.startsWith('data:image/') || 
                   lower.match(/\\.(jpg|jpeg|png|gif|bmp|webp|svg)(\\?|$)/i) ||
                   lower.startsWith('http') && lower.match(/\\.(jpg|jpeg|png|gif|bmp|webp|svg)/i);
        }}
        
        function formatXml(xml) {{
            let formatted = '';
            let indent = '';
            xml.split(/\\>\\s*</).forEach(function(node) {{
                if (node.match(/^\\/\\w/)) indent = indent.substring(2);
                formatted += indent + '<' + node + '>\\n';
                if (node.match(/^<?\\w[^>]*[^\\/]$/)) indent += '  ';
            }});
            return formatted.substring(1, formatted.length - 2);
        }}
        
        function copyModalValue() {{
            if (currentModalValue) {{
                copyToClipboard(currentModalValue);
                showStatus('已复制到剪贴板');
            }}
        }}
        
        function formatJson() {{
            const modalBody = document.getElementById('modalBody');
            const pre = modalBody.querySelector('pre');
            if (pre) {{
                try {{
                    const json = JSON.parse(pre.textContent);
                    pre.textContent = JSON.stringify(json, null, 2);
                    showStatus('JSON已格式化');
                }} catch(e) {{
                    showStatus('不是有效的JSON格式');
                }}
            }}
        }}
        
        function downloadValue() {{
            if (currentModalValue) {{
                let blob, filename, extension;
                
                if (currentModalValue.startsWith('data:image/')) {{
                    const arr = currentModalValue.split(',');
                    const mime = arr[0].match(/:(.*?);/)[1];
                    const bstr = atob(arr[1]);
                    let n = bstr.length;
                    const u8arr = new Uint8Array(n);
                    while(n--){{
                        u8arr[n] = bstr.charCodeAt(n);
                    }}
                    blob = new Blob([u8arr], {{ type: mime }});
                    extension = mime.split('/')[1] || 'png';
                    filename = 'image_' + Date.now() + '.' + extension;
                }} else if (currentModalValue.startsWith('data:application/octet-stream;base64,')) {{
                    const arr = currentModalValue.split(',');
                    const bstr = atob(arr[1]);
                    let n = bstr.length;
                    const u8arr = new Uint8Array(n);
                    while(n--){{
                        u8arr[n] = bstr.charCodeAt(n);
                    }}
                    blob = new Blob([u8arr], {{ type: 'image/png' }});
                    filename = 'image_' + Date.now() + '.png';
                }} else if (currentModalValue.startsWith('data:')) {{
                    const arr = currentModalValue.split(',');
                    const mime = arr[0].match(/:(.*?);/)[1];
                    const bstr = atob(arr[1]);
                    let n = bstr.length;
                    const u8arr = new Uint8Array(n);
                    while(n--){{
                        u8arr[n] = bstr.charCodeAt(n);
                    }}
                    blob = new Blob([u8arr], {{ type: mime }});
                    extension = mime.split('/')[1] || 'bin';
                    filename = 'file_' + Date.now() + '.' + extension;
                }} else {{
                    blob = new Blob([currentModalValue], {{ type: 'text/plain;charset=utf-8' }});
                    filename = 'value_' + Date.now() + '.txt';
                }}
                
                const url = URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = url;
                a.download = filename;
                a.click();
                URL.revokeObjectURL(url);
                showStatus('已下载: ' + filename);
            }}
        }}
        
        document.addEventListener('keydown', function(e) {{
            if (e.key === 'Escape') {{
                closeModal();
            }}
        }});
        
        function showImageModal(dataUrl) {{
            currentModalValue = dataUrl;
            const modal = document.getElementById('valueModal');
            const modalTitle = document.getElementById('modalTitle');
            const modalBody = document.getElementById('modalBody');
            
            modalTitle.textContent = '图片预览';
            
            let imgSrc = dataUrl;
            if (dataUrl.startsWith('data:application/octet-stream;base64,')) {{
                imgSrc = dataUrl.replace('data:application/octet-stream;base64,', 'data:image/png;base64,');
            }}
            
            modalBody.innerHTML = '<img src="' + imgSrc + '" alt="图片" onerror="this.onerror=null;this.parentNode.innerHTML=\\'<div style=\\'color:#f44336;padding:20px;text-align:center;\\'>图片加载失败<br><small>可能不是有效的图片格式</small></div>\\'">';
            modal.classList.add('active');
        }}
        
        function scrollToColumn(colIndex) {{
            const table = document.getElementById('resultTable');
            const headerRow = table.querySelector('thead tr');
            const th = headerRow.children[colIndex + 1];
            
            if (th) {{
                const tableContainer = document.getElementById('tableContainer');
                const scrollLeft = th.offsetLeft - tableContainer.offsetWidth / 2 + th.offsetWidth / 2;
                tableContainer.scrollTo({{
                    left: Math.max(0, scrollLeft),
                    behavior: 'smooth'
                }});
                
                document.querySelectorAll('.column-item').forEach(item => item.classList.remove('selected'));
                const columnItem = document.querySelector(`.column-item[data-column-index="${{colIndex}}"]`);
                if (columnItem) {{
                    columnItem.classList.add('selected');
                }}
                
                highlightColumn(colIndex);
                setTimeout(() => unhighlightColumn(colIndex), 500);
            }}
        }}
        
        function filterColumns() {{
            const searchTerm = document.getElementById('columnSearch').value.toLowerCase();
            const columnItems = document.querySelectorAll('.column-item');
            
            columnItems.forEach(item => {{
                const colName = item.querySelector('.column-name').textContent.toLowerCase();
                if (colName.includes(searchTerm)) {{
                    item.style.display = '';
                }} else {{
                    item.style.display = 'none';
                }}
            }});
        }}
        
        function showColumnContextMenu(event, colIndex, colName) {{
            event.preventDefault();
            const menu = document.getElementById('columnContextMenu');
            menu.dataset.colIndex = colIndex;
            menu.dataset.colName = colName;
            
            let left = event.pageX;
            let top = event.pageY;
            
            if (left + menu.offsetWidth > window.innerWidth) {{
                left = window.innerWidth - menu.offsetWidth - 10;
            }}
            if (top + menu.offsetHeight > window.innerHeight) {{
                top = window.innerHeight - menu.offsetHeight - 10;
            }}
            
            menu.style.left = left + 'px';
            menu.style.top = top + 'px';
            menu.style.display = 'block';
        }}
        
        function copyColumnName() {{
            const menu = document.getElementById('columnContextMenu');
            const colName = menu.dataset.colName;
            copyToClipboard(colName);
            showStatus('已复制列名: ' + colName);
            menu.style.display = 'none';
        }}
        
        function copyColumnValues() {{
            const menu = document.getElementById('columnContextMenu');
            const colIndex = parseInt(menu.dataset.colIndex);
            const values = [];
            
            document.querySelectorAll('#tableBody tr').forEach(row => {{
                if (row.cells[colIndex + 1]) {{
                    values.push(row.cells[colIndex + 1].textContent.trim());
                }}
            }});
            
            copyToClipboard(values.join('\\n'));
            showStatus('已复制 ' + values.length + ' 个值');
            menu.style.display = 'none';
        }}
        
        function initColumnResize() {{
            const table = document.getElementById('resultTable');
            const headerRow = table.querySelector('thead tr');
            const ths = headerRow.querySelectorAll('th');
            
            ths.forEach((th, index) => {{
                const resizeHandle = document.createElement('div');
                resizeHandle.className = 'resize-handle';
                th.appendChild(resizeHandle);
                
                let startX, startWidth;
                
                resizeHandle.addEventListener('mousedown', (e) => {{
                    startX = e.pageX;
                    startWidth = th.offsetWidth;
                    
                    const doDrag = (e) => {{
                        const width = startWidth + (e.pageX - startX);
                        th.style.width = Math.max(50, width) + 'px';
                    }};
                    
                    const stopDrag = () => {{
                        document.removeEventListener('mousemove', doDrag);
                        document.removeEventListener('mouseup', stopDrag);
                    }};
                    
                    document.addEventListener('mousemove', doDrag);
                    document.addEventListener('mouseup', stopDrag);
                    
                    e.preventDefault();
                    e.stopPropagation();
                }});
            }});
        }}
        
        document.addEventListener('DOMContentLoaded', function() {{
            initColumnResize();
        }});
        
        document.addEventListener('click', function(e) {{
            if (!e.target.closest('#columnContextMenu')) {{
                document.getElementById('columnContextMenu').style.display = 'none';
            }}
        }});
        
        function toggleTableGroup(header) {{
            const group = header.closest('.table-group');
            group.classList.toggle('collapsed');
        }}
    </script>
</body>
</html>'''
    
    def _generate_column_list(self, columns_info, column_types=None, column_tables=None):
        if column_types is None:
            column_types = {}
        if column_tables is None:
            column_tables = {}
        
        tables_dict = {}
        for i, col_info in enumerate(columns_info):
            col_name = col_info.get('name', '') if isinstance(col_info, dict) else col_info
            col_type = col_info.get('type', '') if isinstance(col_info, dict) else column_types.get(col_name, '')
            col_table = col_info.get('table', '') if isinstance(col_info, dict) else column_tables.get(col_name, '')
            
            if col_table not in tables_dict:
                tables_dict[col_table] = []
            tables_dict[col_table].append((i, col_name, col_type))
        
        items = []
        for table_name, cols in tables_dict.items():
            table_display = table_name if table_name else '未知表'
            items.append(f'''<div class="table-group">
                <div class="table-group-header" onclick="toggleTableGroup(this)">
                    <span class="toggle-icon">▼</span>
                    <span class="table-name">{html.escape(table_display)}</span>
                    <span class="column-count">{len(cols)} 列</span>
                </div>
                <div class="table-columns">''')
            
            for i, col_name, col_type in cols:
                safe_col = html.escape(str(col_name))
                type_display = f'<span class="column-type">{col_type}</span>' if col_type else ''
                items.append(f'''                <div class="column-item" data-column-index="{i}" onclick="scrollToColumn({i})" oncontextmenu="showColumnContextMenu(event, {i}, '{safe_col}')">
                    <div class="column-icon">T</div>
                    <div class="column-name">{safe_col}</div>
                    {type_display}
                </div>''')
            
            items.append('                </div>\n            </div>')
        
        return '\n'.join(items)
    
    def _generate_filter_row(self, column_names):
        cells = ['<th class="filter-header"><button class="clear-filter-btn" onclick="clearFilters()" title="清空所有筛选">✕</button></th>']
        for i, col in enumerate(column_names):
            cells.append(f'''<th class="filter-header">
                <input type="text" class="filter-input" placeholder="筛选..." data-col-index="{i}" oninput="applyColumnFilter({i}, this.value)">
            </th>''')
        return '\n                    '.join(cells)
    
    def _generate_table_headers(self, column_names):
        headers = []
        for i, col in enumerate(column_names):
            safe_col = html.escape(str(col))
            headers.append(f'<th onclick="sortTable({i + 1})" title="{safe_col}">{safe_col}</th>')
        return '\n                    '.join(headers)
    
    def _generate_table_rows(self, column_names, rows, column_types=None):
        if not rows:
            return f'<tr><td colspan="{len(column_names) + 1}" class="no-data">查询返回 0 行数据</td></tr>'
        
        if column_types is None:
            column_types = {}
        
        row_html = []
        for row_idx, row in enumerate(rows):
            cells = [f'<td class="row-num">{row_idx + 1}</td>']
            for col_idx, col in enumerate(column_names):
                value = row[col_idx] if col_idx < len(row) else ''
                col_type = column_types.get(col, '')
                
                if value is None or value == '':
                    cells.append('<td class="null-value" data-value-type="null">NULL</td>')
                elif col_type in ('BLOB', 'TINYBLOB', 'MEDIUMBLOB', 'LONGBLOB'):
                    if str(value).startswith('data:'):
                        cells.append(f'<td class="image" data-value-type="blob" data-raw-value="{html.escape(str(value))}">[BLOB-图片]</td>')
                    else:
                        cells.append(f'<td class="image" data-value-type="blob">[BLOB]</td>')
                elif col_type in ('TINYTEXT', 'TEXT', 'MEDIUMTEXT', 'LONGTEXT'):
                    str_value = str(value)
                    display = html.escape(str_value[:100]) + '...' if len(str_value) > 100 else html.escape(str_value)
                    cells.append(f'<td class="longtext" data-value-type="longtext" data-raw-value="{html.escape(str_value)}">{display}</td>')
                else:
                    str_value = str(value)
                    value_type = self._get_value_type(value)
                    display_value = self._get_display_value(value, value_type)
                    cells.append(f'<td class="{value_type}" data-value-type="{value_type}" data-raw-value="{html.escape(str_value)}">{display_value}</td>')
            row_html.append(f'<tr data-row-index="{row_idx}" onclick="toggleRowSelection(this, event)">{" ".join(cells)}</tr>')
        return '\n                '.join(row_html)
    
    def _get_value_type(self, value):
        str_val = str(value).strip()
        
        if self._is_image_data(str_val):
            return 'image'
        if self._is_json(str_val):
            return 'json'
        if self._is_html(str_val):
            return 'html'
        if self._is_xml(str_val):
            return 'xml'
        if self._is_number(value):
            return 'number'
        if self._is_datetime(value):
            return 'datetime'
        return 'string'
    
    def _get_display_value(self, value, value_type):
        str_val = str(value)
        
        if value_type == 'image':
            return '[图片]'
        if value_type == 'json':
            return '[JSON] ' + (str_val[:50] + '...' if len(str_val) > 50 else str_val)
        if value_type == 'html':
            return '[HTML] ' + (str_val[:50] + '...' if len(str_val) > 50 else str_val)
        if value_type == 'xml':
            return '[XML] ' + (str_val[:50] + '...' if len(str_val) > 50 else str_val)
        
        if len(str_val) > 200:
            return html.escape(str_val[:200]) + '...'
        return html.escape(str_val)
    
    def _is_image_data(self, value):
        lower = value.lower()
        if lower.startswith('data:image/'):
            return True
        if lower.startswith(('http://', 'https://')):
            import re
            if re.search(r'\.(jpg|jpeg|png|gif|bmp|webp|svg)(\?|$)', lower, re.I):
                return True
        return False
    
    def _is_json(self, value):
        str_val = value.strip()
        if (str_val.startswith('{') and str_val.endswith('}')) or \
           (str_val.startswith('[') and str_val.endswith(']')):
            try:
                import json
                json.loads(str_val)
                return True
            except:
                pass
        return False
    
    def _is_html(self, value):
        str_val = value.strip()
        return str_val.startswith('<') and str_val.endswith('>') and not str_val.startswith('<?xml')
    
    def _is_xml(self, value):
        return value.strip().startswith('<?xml')
    
    def _is_number(self, value):
        try:
            str_val = str(value).strip()
            if str_val == '':
                return False
            float(str_val)
            return True
        except (ValueError, TypeError):
            return False
    
    def _is_datetime(self, value):
        str_val = str(value).strip()
        import re
        patterns = [
            r'^\d{4}-\d{2}-\d{2}',
            r'^\d{4}/\d{2}/\d{2}',
            r'^\d{2}:\d{2}:\d{2}',
            r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}',
            r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',
        ]
        for pattern in patterns:
            if re.match(pattern, str_val):
                return True
        return False


result_viewer = ResultViewerServer()
