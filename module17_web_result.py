import json
import threading
import socket
import webbrowser
import time
import pymysql
import base64
import re
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse
import html
import threading
from datetime import datetime, timedelta


def sqlyog_decode(base64str):
    tmp = base64.b64decode(base64str)
    return bytearray([(b << 1 & 255) | (b >> 7) for b in tmp]).decode("utf8")


class ConnectionManager:
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if hasattr(self, '_initialized') and self._initialized:
            return
        self._initialized = True
        self.connections = {}
        self.last_activity = {}
        self.timers = {}
        self.timeout = 180  # 3 minutes
    
    def add_connection(self, conn_id, connection):
        self.connections[conn_id] = connection
        self.last_activity[conn_id] = time.time()
        self._reset_timer(conn_id)
    
    def get_connection(self, conn_id):
        if conn_id in self.connections:
            self.last_activity[conn_id] = time.time()
            self._reset_timer(conn_id)
            return self.connections[conn_id]
        return None
    
    def remove_connection(self, conn_id):
        if conn_id in self.timers:
            self.timers[conn_id].cancel()
            del self.timers[conn_id]
        if conn_id in self.connections:
            try:
                self.connections[conn_id].close()
            except:
                pass
            del self.connections[conn_id]
            del self.last_activity[conn_id]
    
    def _reset_timer(self, conn_id):
        if conn_id in self.timers:
            self.timers[conn_id].cancel()
        self.timers[conn_id] = threading.Timer(self.timeout, self.remove_connection, [conn_id])
        self.timers[conn_id].daemon = True
        self.timers[conn_id].start()
    
    def cleanup(self):
        for conn_id in list(self.connections.keys()):
            self.remove_connection(conn_id)


conn_manager = ConnectionManager()


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
        self.current_db_config = None
        self.conn_counter = 0
    
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
            conn_manager.cleanup()
    
    def open_web_console(self, db_config):
        self.current_db_config = db_config
        if self.port is None:
            self.start_server()
        
        url = f'http://127.0.0.1:{self.port}/'
        webbrowser.open(url)
        return url
    
    def add_result(self, columns, rows, query_info=None):
        pass
    
    def get_result(self, result_id):
        pass
    
    def open_browser(self, result_id):
        pass


result_viewer = ResultViewerServer()


class ResultViewerHandler(BaseHTTPRequestHandler):
    server_instance = None
    
    def log_message(self, format, *args):
        pass
    
    def do_GET(self):
        parsed_path = urlparse(self.path)
        
        if parsed_path.path == '/' or parsed_path.path == '':
            self._serve_console_page()
        elif parsed_path.path == '/api/connect':
            self._handle_connect(parsed_path)
        elif parsed_path.path == '/api/tables':
            self._handle_get_tables(parsed_path)
        elif parsed_path.path == '/api/table_info':
            self._handle_table_info(parsed_path)
        elif parsed_path.path == '/api/execute':
            self._handle_execute(parsed_path)
        elif parsed_path.path == '/api/disconnect':
            self._handle_disconnect(parsed_path)
        else:
            self._serve_error_page("页面未找到")
    
    def do_POST(self):
        parsed_path = urlparse(self.path)
        content_length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_length)
        
        try:
            data = json.loads(body.decode('utf-8'))
        except:
            data = {}
        
        if parsed_path.path == '/api/connect':
            self._handle_connect_post(data)
        elif parsed_path.path == '/api/execute':
            self._handle_execute_post(data)
        else:
            self.send_error(404)
    
    def _serve_console_page(self):
        html_content = '''<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MySQL 只读查询器</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Segoe UI', 'Microsoft YaHei UI', Arial, sans-serif;
            background: #f5f5f5;
            color: #333;
            font-size: 12px;
            overflow: hidden;
            height: 100vh;
            display: flex;
            flex-direction: column;
        }
        
        .main-container {
            display: flex;
            flex: 1;
            overflow: hidden;
        }
        
        .sidebar {
            width: 250px;
            background: #fff;
            border-right: 1px solid #ddd;
            display: flex;
            flex-direction: column;
            flex-shrink: 0;
        }
        
        .sidebar-header {
            padding: 10px 12px;
            background: #f5f5f5;
            border-bottom: 1px solid #ddd;
            font-weight: 600;
            color: #333;
            font-size: 13px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        
        .sidebar-toolbar {
            padding: 8px;
            border-bottom: 1px solid #eee;
            background: #fafafa;
        }
        
        .btn {
            background: #fff;
            border: 1px solid #ccc;
            padding: 5px 10px;
            border-radius: 3px;
            cursor: pointer;
            font-size: 11px;
            color: #333;
            display: inline-flex;
            align-items: center;
            gap: 4px;
            transition: all 0.15s;
        }
        
        .btn:hover {
            background: #e8e8e8;
            border-color: #aaa;
        }
        
        .btn.primary {
            background: #4a90d9;
            color: #fff;
            border-color: #3a7bc8;
        }
        
        .btn.primary:hover {
            background: #3a7bc8;
        }
        
        .btn.danger {
            background: #d9534f;
            color: #fff;
            border-color: #c9302c;
        }
        
        .btn.danger:hover {
            background: #c9302c;
        }
        
        .db-tree {
            flex: 1;
            overflow-y: auto;
            padding: 5px 0;
        }
        
        .tree-item {
            padding: 4px 10px;
            cursor: pointer;
            display: flex;
            align-items: center;
            gap: 6px;
            border-left: 3px solid transparent;
            font-size: 12px;
        }
        
        .tree-item:hover {
            background: #e8f4fc;
            border-left-color: #4a90d9;
        }
        
        .tree-item.selected {
            background: #d0e8f8;
            border-left-color: #4a90d9;
        }
        
        .tree-folder {
            font-weight: 600;
            color: #555;
        }
        
        .tree-table {
            color: #333;
        }
        
        .tree-icon {
            font-size: 12px;
        }
        
        .content-area {
            flex: 1;
            display: flex;
            flex-direction: column;
            overflow: hidden;
        }
        
        .toolbar {
            background: #f8f8f8;
            border-bottom: 1px solid #ddd;
            padding: 6px 10px;
            display: flex;
            align-items: center;
            gap: 8px;
            flex-shrink: 0;
        }
        
        .toolbar-separator {
            width: 1px;
            height: 20px;
            background: #ddd;
            margin: 0 4px;
        }
        
        .sql-editor-container {
            flex: 1;
            display: flex;
            flex-direction: column;
            overflow: hidden;
            border-bottom: 1px solid #ddd;
        }
        
        .sql-editor-header {
            background: #f5f5f5;
            padding: 4px 10px;
            border-bottom: 1px solid #ddd;
            display: flex;
            align-items: center;
            gap: 8px;
        }
        
        .sql-editor-tabs {
            display: flex;
            gap: 2px;
        }
        
        .editor-tab {
            padding: 5px 12px;
            background: #e8e8e8;
            border: 1px solid #ccc;
            border-bottom: none;
            border-radius: 3px 3px 0 0;
            cursor: pointer;
            font-size: 11px;
            display: flex;
            align-items: center;
            gap: 6px;
        }
        
        .editor-tab.active {
            background: #fff;
            border-bottom: 1px solid #fff;
            margin-bottom: -1px;
        }
        
        .editor-tab:hover {
            background: #ddd;
        }
        
        .editor-tab.active:hover {
            background: #fff;
        }
        
        .tab-close {
            font-size: 12px;
            opacity: 0.6;
            cursor: pointer;
        }
        
        .tab-close:hover {
            opacity: 1;
            color: #d9534f;
        }
        
        .sql-editor {
            flex: 1;
            display: flex;
            flex-direction: column;
            background: #1e1e1e;
            color: #d4d4d4;
            font-family: 'Consolas', 'Monaco', 'Courier New', monospace;
            font-size: 13px;
            overflow: hidden;
        }
        
        .sql-textarea {
            flex: 1;
            background: #1e1e1e;
            color: #d4d4d4;
            border: none;
            padding: 10px;
            font-family: inherit;
            font-size: inherit;
            resize: none;
            outline: none;
            line-height: 1.5;
        }
        
        .results-container {
            height: 40%;
            min-height: 150px;
            display: flex;
            flex-direction: column;
            overflow: hidden;
            background: #fff;
        }
        
        .results-header {
            background: #f5f5f5;
            padding: 5px 10px;
            border-bottom: 1px solid #ddd;
            display: flex;
            justify-content: space-between;
            align-items: center;
            font-size: 11px;
        }
        
        .results-tabs {
            display: flex;
            gap: 2px;
        }
        
        .results-tab {
            padding: 3px 10px;
            background: #e8e8e8;
            border: 1px solid #ccc;
            border-bottom: none;
            border-radius: 3px 3px 0 0;
            cursor: pointer;
            font-size: 11px;
        }
        
        .results-tab.active {
            background: #fff;
            border-bottom: 1px solid #fff;
            margin-bottom: -1px;
        }
        
        .table-container {
            flex: 1;
            overflow: auto;
        }
        
        table {
            border-collapse: collapse;
            width: 100%;
            min-width: max-content;
        }
        
        th {
            background: linear-gradient(to bottom, #f5f5f5 0%, #e8e8e8 100%);
            border: 1px solid #ccc;
            padding: 6px 10px;
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
        }
        
        th:hover {
            background: linear-gradient(to bottom, #e8e8e8 0%, #d8d8d8 100%);
        }
        
        th.row-num {
            width: 50px;
            text-align: center;
            background: linear-gradient(to bottom, #f0f0f0 0%, #e0e0e0 100%);
            color: #666;
        }
        
        td {
            border: 1px solid #e0e0e0;
            padding: 4px 8px;
            font-family: 'Consolas', 'Monaco', 'Courier New', monospace;
            font-size: 12px;
            white-space: pre;
            max-width: 300px;
            overflow: hidden;
            text-overflow: ellipsis;
            color: #222;
        }
        
        td.row-num {
            text-align: center;
            color: #666;
            background: #fafafa;
            font-family: 'Segoe UI', Arial, sans-serif;
            font-size: 11px;
            min-width: 50px;
        }
        
        tr:nth-child(even) td:not(.row-num) {
            background: #fafafa;
        }
        
        tr:hover td {
            background: #e3f2fd !important;
        }
        
        td.null-value {
            color: #999;
            font-style: italic;
        }
        
        td.number {
            text-align: right;
            color: #0055aa;
            font-weight: 500;
        }
        
        td.string {
            color: #222;
        }
        
        td.datetime {
            color: #007700;
        }
        
        .status-bar {
            background: #4a90d9;
            padding: 4px 12px;
            font-size: 11px;
            color: #fff;
            display: flex;
            align-items: center;
            gap: 20px;
            flex-shrink: 0;
        }
        
        .status-item {
            display: flex;
            align-items: center;
            gap: 4px;
        }
        
        .status-dot {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: #5cb85c;
        }
        
        .status-dot.disconnected {
            background: #d9534f;
        }
        
        .autocomplete-dropdown {
            position: absolute;
            background: #fff;
            border: 1px solid #ccc;
            border-radius: 3px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
            z-index: 1000;
            display: none;
            max-height: 200px;
            overflow-y: auto;
        }
        
        .autocomplete-item {
            padding: 5px 10px;
            cursor: pointer;
            font-family: 'Consolas', monospace;
            font-size: 12px;
        }
        
        .autocomplete-item:hover,
        .autocomplete-item.selected {
            background: #4a90d9;
            color: #fff;
        }
        
        .autocomplete-item .type {
            color: #888;
            font-size: 10px;
            margin-left: 10px;
        }
        
        .autocomplete-item:hover .type,
        .autocomplete-item.selected .type {
            color: #ccc;
        }
        
        .no-data {
            text-align: center;
            padding: 40px;
            color: #666;
            font-style: italic;
        }
        
        .template-panel {
            width: 200px;
            background: #fff;
            border-left: 1px solid #ddd;
            display: flex;
            flex-direction: column;
            flex-shrink: 0;
        }
        
        .template-header {
            padding: 10px 12px;
            background: #f5f5f5;
            border-bottom: 1px solid #ddd;
            font-weight: 600;
            color: #333;
            font-size: 13px;
        }
        
        .template-list {
            flex: 1;
            overflow-y: auto;
            padding: 5px 0;
        }
        
        .template-item {
            padding: 6px 10px;
            cursor: pointer;
            font-size: 12px;
            border-left: 3px solid transparent;
        }
        
        .template-item:hover {
            background: #e8f4fc;
            border-left-color: #4a90d9;
        }
        
        .connection-info {
            padding: 8px 10px;
            background: #fafafa;
            border-bottom: 1px solid #eee;
            font-size: 11px;
        }
        
        .connection-name {
            font-weight: 600;
            color: #333;
        }
        
        .connection-detail {
            color: #666;
            margin-top: 2px;
        }
    </style>
</head>
<body>
    <div class="main-container">
        <div class="sidebar">
            <div class="sidebar-header">
                <span>数据库导航</span>
            </div>
            <div class="sidebar-toolbar">
                <button class="btn" onclick="connect()" id="connectBtn">
                    <span>🔌</span> 连接
                </button>
                <button class="btn danger" onclick="disconnect()" id="disconnectBtn" style="display:none;">
                    <span>❌</span> 断开
                </button>
                <div class="toolbar-separator"></div>
                <button class="btn" onclick="refreshTables()" id="refreshBtn" style="display:none;">
                    <span>🔄</span> 刷新
                </button>
            </div>
            <div class="connection-info" id="connectionInfo" style="display:none;">
                <div class="connection-name" id="connName"></div>
                <div class="connection-detail" id="connDetail"></div>
            </div>
            <div class="db-tree" id="dbTree">
                <div class="no-data">请先连接数据库</div>
            </div>
        </div>
        
        <div class="content-area">
            <div class="toolbar">
                <button class="btn primary" onclick="executeSQL()" id="executeBtn">
                    <span>▶</span> 执行 (F9)
                </button>
                <div class="toolbar-separator"></div>
                <button class="btn" onclick="formatSQL()">
                    <span>📝</span> 格式化
                </button>
                <button class="btn" onclick="clearSQL()">
                    <span>🗑️</span> 清空
                </button>
            </div>
            
            <div class="sql-editor-container">
                <div class="sql-editor-header">
                    <div class="sql-editor-tabs" id="editorTabs">
                        <div class="editor-tab active" data-tab="1">
                            <span>查询 1</span>
                            <span class="tab-close" onclick="closeTab(event, 1)">×</span>
                        </div>
                    </div>
                    <button class="btn" style="margin-left: 10px;" onclick="newTab()">
                        <span>+</span> 新标签
                    </button>
                </div>
                <div class="sql-editor">
                    <textarea class="sql-textarea" id="sqlEditor" placeholder="输入SQL查询... (仅支持SELECT语句)" spellcheck="false"></textarea>
                    <div class="autocomplete-dropdown" id="autocomplete"></div>
                </div>
            </div>
            
            <div class="results-container">
                <div class="results-header">
                    <div class="results-tabs" id="resultsTabs">
                        <div class="results-tab active">结果集</div>
                    </div>
                    <div id="resultsInfo">等待执行查询...</div>
                </div>
                <div class="table-container" id="resultsTable">
                    <div class="no-data">执行查询后结果将显示在这里</div>
                </div>
            </div>
        </div>
        
        <div class="template-panel">
            <div class="template-header">SQL 模板</div>
            <div class="template-list" id="templateList">
                <div class="template-item" onclick="insertTemplate('SELECT * FROM table_name LIMIT 100;')">
                    查询前100行
                </div>
                <div class="template-item" onclick="insertTemplate('SELECT COUNT(*) FROM table_name;')">
                    统计行数
                </div>
                <div class="template-item" onclick="insertTemplate('DESCRIBE table_name;')">
                    查看表结构
                </div>
                <div class="template-item" onclick="insertTemplate('SHOW TABLES;')">
                    查看所有表
                </div>
                <div class="template-item" onclick="insertTemplate('SHOW CREATE TABLE table_name;')">
                    查看建表语句
                </div>
            </div>
        </div>
    </div>
    
    <div class="status-bar">
        <div class="status-item">
            <span class="status-dot disconnected" id="statusDot"></span>
            <span id="statusText">未连接</span>
        </div>
        <div class="status-item" id="dbStatus">
            <span>📊</span>
            <span id="dbName">-</span>
        </div>
        <div class="status-item" style="margin-left: auto;">
            <span id="execTime">-</span>
        </div>
    </div>
    
    <script>
        let connectionId = null;
        let tables = [];
        let procedures = [];
        let functions = [];
        let columns = {};
        let currentTab = 1;
        let tabCounter = 1;
        let tabContents = {1: ''};
        let resultsData = null;
        
        const sqlKeywords = ['SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'OUTER', 
                           'ON', 'GROUP', 'BY', 'HAVING', 'ORDER', 'LIMIT', 'OFFSET', 'DISTINCT', 'AS',
                           'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'IF', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
                           'IN', 'NOT', 'LIKE', 'BETWEEN', 'IS', 'NULL', 'DESCRIBE', 'SHOW', 'TABLES', 'CREATE'];
        
        const sqlEditor = document.getElementById('sqlEditor');
        const autocomplete = document.getElementById('autocomplete');
        let autocompleteIndex = -1;
        let autocompleteItems = [];
        
        sqlEditor.addEventListener('keydown', function(e) {
            if (e.key === 'F9') {
                e.preventDefault();
                executeSQL();
            } else if (e.key === 'Tab') {
                e.preventDefault();
                const start = this.selectionStart;
                const end = this.selectionEnd;
                this.value = this.value.substring(0, start) + '    ' + this.value.substring(end);
                this.selectionStart = this.selectionEnd = start + 4;
            } else if (e.key === 'ArrowDown' && autocomplete.style.display === 'block') {
                e.preventDefault();
                autocompleteIndex = Math.min(autocompleteIndex + 1, autocompleteItems.length - 1);
                updateAutocompleteSelection();
            } else if (e.key === 'ArrowUp' && autocomplete.style.display === 'block') {
                e.preventDefault();
                autocompleteIndex = Math.max(autocompleteIndex - 1, 0);
                updateAutocompleteSelection();
            } else if (e.key === 'Enter' && autocomplete.style.display === 'block') {
                e.preventDefault();
                if (autocompleteIndex >= 0) {
                    insertAutocomplete(autocompleteItems[autocompleteIndex]);
                }
            } else if (e.key === 'Escape') {
                hideAutocomplete();
            }
        });
        
        sqlEditor.addEventListener('input', function() {
            tabContents[currentTab] = this.value;
            showAutocomplete();
        });
        
        sqlEditor.addEventListener('click', hideAutocomplete);
        
        function showAutocomplete() {
            const text = sqlEditor.value.substring(0, sqlEditor.selectionStart);
            const match = text.match(/[\w.]*$/);
            if (!match || match[0].length < 1) {
                hideAutocomplete();
                return;
            }
            
            const word = match[0].toLowerCase();
            autocompleteItems = [];
            
            for (const kw of sqlKeywords) {
                if (kw.toLowerCase().startsWith(word)) {
                    autocompleteItems.push({value: kw, type: 'KEYWORD'});
                }
            }
            
            for (const table of tables) {
                if (table.toLowerCase().startsWith(word)) {
                    autocompleteItems.push({value: table, type: 'TABLE'});
                }
            }
            
            for (const table in columns) {
                for (const col of columns[table]) {
                    if (col.toLowerCase().startsWith(word)) {
                        autocompleteItems.push({value: col, type: 'COLUMN', table: table});
                    }
                }
            }
            
            if (autocompleteItems.length > 0) {
                autocompleteIndex = 0;
                renderAutocomplete();
            } else {
                hideAutocomplete();
            }
        }
        
        function renderAutocomplete() {
            autocomplete.innerHTML = autocompleteItems.map((item, i) => 
                `<div class="autocomplete-item ${i === autocompleteIndex ? 'selected' : ''}" 
                     onclick="insertAutocomplete(autocompleteItems[${i}])" 
                     data-index="${i}">
                    ${item.value}
                    <span class="type">${item.type}</span>
                </div>`
            ).join('');
            
            const rect = sqlEditor.getBoundingClientRect();
            const lines = sqlEditor.value.substring(0, sqlEditor.selectionStart).split('\\n');
            const lineHeight = 20;
            autocomplete.style.top = (rect.top + lines.length * lineHeight + 5) + 'px';
            autocomplete.style.left = rect.left + 'px';
            autocomplete.style.display = 'block';
        }
        
        function updateAutocompleteSelection() {
            const items = autocomplete.querySelectorAll('.autocomplete-item');
            items.forEach((item, i) => {
                item.classList.toggle('selected', i === autocompleteIndex);
            });
        }
        
        function insertAutocomplete(item) {
            const text = sqlEditor.value;
            const start = sqlEditor.selectionStart;
            const before = text.substring(0, start);
            const after = text.substring(start);
            const wordMatch = before.match(/[\\w.]*$/);
            const newBefore = before.substring(0, before.length - wordMatch[0].length) + item.value;
            sqlEditor.value = newBefore + after;
            sqlEditor.selectionStart = sqlEditor.selectionEnd = newBefore.length;
            tabContents[currentTab] = sqlEditor.value;
            hideAutocomplete();
            sqlEditor.focus();
        }
        
        function hideAutocomplete() {
            autocomplete.style.display = 'none';
            autocompleteIndex = -1;
        }
        
        async function connect() {
            try {
                const response = await fetch('/api/connect', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'}
                });
                const result = await response.json();
                
                if (result.success) {
                    connectionId = result.connection_id;
                    document.getElementById('connectBtn').style.display = 'none';
                    document.getElementById('disconnectBtn').style.display = 'inline-flex';
                    document.getElementById('refreshBtn').style.display = 'inline-flex';
                    document.getElementById('connectionInfo').style.display = 'block';
                    document.getElementById('connName').textContent = result.db_name;
                    document.getElementById('connDetail').textContent = result.host + ':' + result.port;
                    document.getElementById('dbName').textContent = result.db_name;
                    document.getElementById('statusDot').classList.remove('disconnected');
                    document.getElementById('statusText').textContent = '已连接';
                    await refreshTables();
                } else {
                    alert('连接失败: ' + result.error);
                }
            } catch (e) {
                alert('连接错误: ' + e.message);
            }
        }
        
        async function disconnect() {
            if (!connectionId) return;
            
            try {
                await fetch('/api/disconnect?conn_id=' + connectionId);
            } catch (e) {}
            
            connectionId = null;
            tables = [];
            columns = {};
            document.getElementById('connectBtn').style.display = 'inline-flex';
            document.getElementById('disconnectBtn').style.display = 'none';
            document.getElementById('refreshBtn').style.display = 'none';
            document.getElementById('connectionInfo').style.display = 'none';
            document.getElementById('dbName').textContent = '-';
            document.getElementById('statusDot').classList.add('disconnected');
            document.getElementById('statusText').textContent = '未连接';
            document.getElementById('dbTree').innerHTML = '<div class="no-data">请先连接数据库</div>';
        }
        
        async function refreshTables() {
            if (!connectionId) return;
            
            try {
                const response = await fetch('/api/tables?conn_id=' + connectionId);
                const result = await response.json();
                
                if (result.success) {
                    tables = result.tables || [];
                    procedures = result.procedures || [];
                    functions = result.functions || [];
                    columns = result.columns || {};
                    renderTree();
                }
            } catch (e) {
                console.error('Failed to refresh tables:', e);
            }
        }
        
        function renderTree() {
            const tree = document.getElementById('dbTree');
            let html = '<div class="tree-item tree-folder" onclick="toggleFolder(this)">';
            html += '<span class="tree-icon">📁</span>';
            html += '<span>表 (' + tables.length + ')</span>';
            html += '</div>';
            html += '<div class="tree-children">';
            
            for (const table of tables) {
                html += '<div class="tree-item tree-table" onclick="selectTable(\\'' + table + '\\')" ondblclick="insertTable(\\'' + table + '\\')">';
                html += '<span class="tree-icon">📄</span>';
                html += '<span>' + table + '</span>';
                html += '</div>';
            }
            
            html += '</div>';
            tree.innerHTML = html;
        }
        
        function toggleFolder(element) {
            const children = element.nextElementSibling;
            if (children) {
                children.style.display = children.style.display === 'none' ? 'block' : 'none';
            }
        }
        
        function selectTable(table) {
            const items = document.querySelectorAll('.tree-item');
            items.forEach(item => item.classList.remove('selected'));
            event.currentTarget.classList.add('selected');
        }
        
        function insertTable(table) {
            sqlEditor.value += 'SELECT * FROM ' + table + ' LIMIT 100;';
            sqlEditor.focus();
        }
        
        async function executeSQL() {
            if (!connectionId) {
                alert('请先连接数据库');
                return;
            }
            
            const sql = sqlEditor.value.trim();
            if (!sql) {
                alert('请输入SQL语句');
                return;
            }
            
            const startTime = Date.now();
            document.getElementById('resultsInfo').textContent = '正在执行...';
            
            try {
                const response = await fetch('/api/execute', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        conn_id: connectionId,
                        sql: sql
                    })
                });
                
                // 检查响应是否为JSON格式
                const contentType = response.headers.get('content-type');
                if (!contentType || !contentType.includes('application/json')) {
                    throw new Error('服务器返回非JSON响应');
                }
                
                const result = await response.json();
                const elapsed = (Date.now() - startTime) / 1000;
                
                if (result.success) {
                    resultsData = result;
                    renderResults(result);
                    document.getElementById('resultsInfo').textContent = 
                        '返回 ' + result.rows.length + ' 行, 耗时 ' + elapsed.toFixed(3) + 's';
                    document.getElementById('execTime').textContent = '执行时间: ' + elapsed.toFixed(3) + 's';
                } else {
                    document.getElementById('resultsTable').innerHTML = 
                        '<div class="no-data" style="color: #d9534f;">错误: ' + result.error + '</div>';
                    document.getElementById('resultsInfo').textContent = '执行失败';
                }
            } catch (e) {
                document.getElementById('resultsTable').innerHTML = 
                    '<div class="no-data" style="color: #d9534f;">错误: ' + e.message + '</div>';
                document.getElementById('resultsInfo').textContent = '执行失败';
            }
        }
        
        function renderResults(result) {
            const container = document.getElementById('resultsTable');
            
            if (!result.columns || result.columns.length === 0) {
                container.innerHTML = '<div class="no-data">无数据</div>';
                return;
            }
            
            let html = '<table><thead><tr>';
            html += '<th class="row-num">#</th>';
            for (const col of result.columns) {
                html += '<th>' + htmlEscape(col) + '</th>';
            }
            html += '</tr></thead><tbody>';
            
            for (let i = 0; i < result.rows.length; i++) {
                html += '<tr>';
                html += '<td class="row-num">' + (i + 1) + '</td>';
                for (let j = 0; j < result.columns.length; j++) {
                    const value = result.rows[i][j];
                    if (value === null) {
                        html += '<td class="null-value">NULL</td>';
                    } else {
                        html += '<td>' + htmlEscape(String(value)) + '</td>';
                    }
                }
                html += '</tr>';
            }
            
            html += '</tbody></table>';
            container.innerHTML = html;
        }
        
        function htmlEscape(text) {
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }
        
        function formatSQL() {
            let sql = sqlEditor.value;
            sql = sql.replace(/\\s+/g, ' ').trim();
            for (const kw of sqlKeywords) {
                const regex = new RegExp('\\\\b' + kw + '\\\\b', 'gi');
                sql = sql.replace(regex, kw);
            }
            sqlEditor.value = sql;
            tabContents[currentTab] = sql;
        }
        
        function clearSQL() {
            sqlEditor.value = '';
            tabContents[currentTab] = '';
            sqlEditor.focus();
        }
        
        function newTab() {
            tabCounter++;
            const newTabId = tabCounter;
            currentTab = newTabId;
            tabContents[newTabId] = '';
            
            const tabs = document.getElementById('editorTabs');
            const newTabEl = document.createElement('div');
            newTabEl.className = 'editor-tab';
            newTabEl.dataset.tab = newTabId;
            newTabEl.innerHTML = '<span>查询 ' + newTabId + '</span>' +
                '<span class="tab-close" onclick="closeTab(event, ' + newTabId + ')">×</span>';
            newTabEl.onclick = function(e) {
                if (!e.target.classList.contains('tab-close')) {
                    switchTab(newTabId);
                }
            };
            tabs.appendChild(newTabEl);
            
            switchTab(newTabId);
        }
        
        function switchTab(tabId) {
            tabContents[currentTab] = sqlEditor.value;
            currentTab = tabId;
            
            document.querySelectorAll('.editor-tab').forEach(tab => {
                tab.classList.toggle('active', parseInt(tab.dataset.tab) === tabId);
            });
            
            sqlEditor.value = tabContents[tabId] || '';
        }
        
        function closeTab(event, tabId) {
            event.stopPropagation();
            if (Object.keys(tabContents).length <= 1) {
                return;
            }
            
            delete tabContents[tabId];
            event.currentTarget.closest('.editor-tab').remove();
            
            if (currentTab === tabId) {
                const remainingTabs = Object.keys(tabContents);
                if (remainingTabs.length > 0) {
                    switchTab(parseInt(remainingTabs[0]));
                }
            }
        }
        
        function insertTemplate(sql) {
            sqlEditor.value = sql;
            tabContents[currentTab] = sql;
            sqlEditor.focus();
        }
        
        window.addEventListener('beforeunload', function() {
            if (connectionId) {
                fetch('/api/disconnect?conn_id=' + connectionId);
            }
        });
    </script>
</body>
</html>'''
        
        self.send_response(200)
        self.send_header('Content-type', 'text/html; charset=utf-8')
        self.end_headers()
        self.wfile.write(html_content.encode('utf-8'))
    
    def _handle_connect_post(self, data):
        try:
            db_config = self.server_instance.current_db_config
            if not db_config:
                self._send_json({'success': False, 'error': 'No database configuration'})
                return
            
            host = db_config.get('host', '')
            port = int(db_config.get('port', 3306))
            user = db_config.get('user', '')
            password = db_config.get('password', '')
            database = db_config.get('database', '')
            ssh_enabled = db_config.get('ssh_enabled', 0)
            
            if db_config.get('password_encrypted', 0) == 1:
                try:
                    password = sqlyog_decode(password)
                except:
                    pass
            
            if ssh_enabled == 1:
                # 尝试建立SSH隧道连接
                ssh_host = db_config.get('ssh_host', '')
                ssh_port = int(db_config.get('ssh_port', 22))
                ssh_user = db_config.get('ssh_user', '')
                ssh_password = db_config.get('ssh_password', '')
                
                if db_config.get('ssh_password_encrypted', 0) == 1:
                    try:
                        ssh_password = sqlyog_decode(ssh_password)
                    except:
                        pass
                
                try:
                    from sshtunnel import SSHTunnelForwarder
                    
                    # 创建SSH隧道
                    tunnel = SSHTunnelForwarder(
                        (ssh_host, ssh_port),
                        ssh_username=ssh_user,
                        ssh_password=ssh_password,
                        remote_bind_address=(host, port)
                    )
                    
                    # 启动隧道
                    tunnel.start()
                    
                    # 通过隧道连接数据库
                    conn = pymysql.connect(
                        host='127.0.0.1',
                        port=tunnel.local_bind_port,
                        user=user,
                        password=password,
                        database=database,
                        charset='utf8mb4',
                        cursorclass=pymysql.cursors.DictCursor,
                        connect_timeout=10
                    )
                    
                    # 存储隧道信息，以便在断开连接时关闭
                    self.server_instance.tunnels = getattr(self.server_instance, 'tunnels', {})
                    
                except ImportError:
                    self._send_json({'success': False, 'error': 'SSHTunnel module not installed'})
                    return
                except Exception as e:
                    self._send_json({'success': False, 'error': f'SSH tunnel connection failed: {str(e)}'})
                    return
            else:
                # 直接连接数据库
                conn = pymysql.connect(
                    host=host,
                    port=port,
                    user=user,
                    password=password,
                    database=database,
                    charset='utf8mb4',
                    cursorclass=pymysql.cursors.DictCursor,
                    connect_timeout=10
                )
            
            self.server_instance.conn_counter += 1
            conn_id = f"conn_{self.server_instance.conn_counter}"
            conn_manager.add_connection(conn_id, conn)
            
            # 如果使用了SSH隧道，存储隧道信息
            if ssh_enabled == 1:
                self.server_instance.tunnels[conn_id] = tunnel
            
            self._send_json({
                'success': True,
                'connection_id': conn_id,
                'db_name': database,
                'host': host,
                'port': port
            })
            
        except Exception as e:
            self._send_json({'success': False, 'error': str(e)})
    
    def _handle_connect(self, parsed_path):
        self._handle_connect_post({})
    
    def _handle_get_tables(self, parsed_path):
        try:
            params = parse_qs(parsed_path.query)
            conn_id = params.get('conn_id', [''])[0]
            
            conn = conn_manager.get_connection(conn_id)
            if not conn:
                self._send_json({'success': False, 'error': 'Connection not found'})
                return
            
            with conn.cursor() as cursor:
                # 获取表
                cursor.execute("SHOW TABLES")
                tables_result = cursor.fetchall()
                tables = [list(row.values())[0] for row in tables_result]
                
                # 获取存储过程
                cursor.execute("SHOW PROCEDURE STATUS WHERE Db = DATABASE()")
                procedures_result = cursor.fetchall()
                procedures = [row['Name'] for row in procedures_result]
                
                # 获取函数
                cursor.execute("SHOW FUNCTION STATUS WHERE Db = DATABASE()")
                functions_result = cursor.fetchall()
                functions = [row['Name'] for row in functions_result]
                
                columns = {}
                for table in tables:
                    try:
                        cursor.execute(f"DESCRIBE `{table}`")
                        cols = cursor.fetchall()
                        columns[table] = [col['Field'] for col in cols]
                    except:
                        columns[table] = []
            
            self._send_json({'success': True, 'tables': tables, 'procedures': procedures, 'functions': functions, 'columns': columns})
            
        except Exception as e:
            self._send_json({'success': False, 'error': str(e)})
    
    def _handle_table_info(self, parsed_path):
        try:
            params = parse_qs(parsed_path.query)
            conn_id = params.get('conn_id', [''])[0]
            table = params.get('table', [''])[0]
            
            conn = conn_manager.get_connection(conn_id)
            if not conn:
                self._send_json({'success': False, 'error': 'Connection not found'})
                return
            
            with conn.cursor() as cursor:
                cursor.execute(f"DESCRIBE `{table}`")
                columns = cursor.fetchall()
            
            self._send_json({'success': True, 'columns': columns})
            
        except Exception as e:
            self._send_json({'success': False, 'error': str(e)})
    
    def _handle_execute_post(self, data):
        try:
            conn_id = data.get('conn_id', '')
            sql = data.get('sql', '')
            
            if not sql.strip():
                self._send_json({'success': False, 'error': 'SQL is empty'})
                return
            
            sql_lower = sql.strip().lower()
            if not (sql_lower.startswith('select') or sql_lower.startswith('show') or 
                    sql_lower.startswith('describe') or sql_lower.startswith('desc')):
                self._send_json({'success': False, 'error': 'Only SELECT, SHOW, DESCRIBE statements are allowed'})
                return
            
            conn = conn_manager.get_connection(conn_id)
            if not conn:
                self._send_json({'success': False, 'error': 'Connection not found'})
                return
            
            with conn.cursor() as cursor:
                cursor.execute(sql)
                rows = cursor.fetchall()
                
                columns = []
                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                
                formatted_rows = []
                for row in rows:
                    formatted_row = []
                    for col in columns:
                        value = row.get(col)
                        if isinstance(value, bytes):
                            try:
                                value = value.decode('utf-8')
                            except:
                                value = '[BINARY]'
                        elif isinstance(value, datetime):
                            value = value.strftime('%Y-%m-%d %H:%M:%S')
                        formatted_row.append(value)
                    formatted_rows.append(formatted_row)
            
            self._send_json({
                'success': True,
                'columns': columns,
                'rows': formatted_rows
            })
            
        except Exception as e:
            self._send_json({'success': False, 'error': str(e)})
    
    def _handle_execute(self, parsed_path):
        self._handle_execute_post({})
    
    def _handle_disconnect(self, parsed_path):
        try:
            params = parse_qs(parsed_path.query)
            conn_id = params.get('conn_id', [''])[0]
            
            # 关闭SSH隧道（如果存在）
            if hasattr(self.server_instance, 'tunnels') and conn_id in self.server_instance.tunnels:
                try:
                    tunnel = self.server_instance.tunnels[conn_id]
                    tunnel.stop()
                    del self.server_instance.tunnels[conn_id]
                except:
                    pass
            
            # 移除数据库连接
            conn_manager.remove_connection(conn_id)
            
            self._send_json({'success': True})
        except Exception as e:
            self._send_json({'success': False, 'error': str(e)})
    
    def _send_json(self, data):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode('utf-8'))
    
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
