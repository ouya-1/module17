import tkinter as tk
from tkinter import ttk, scrolledtext, filedialog
import re
import time
import base64
import pymysql
from contextlib import contextmanager
import configparser
import platform
import subprocess
import socket

from extra.custom_dialog import CustomDialog
from extra.module_execute import execute_sql
from extra.module17_web_result import result_viewer


def sqlyog_decode(base64str):
    tmp = base64.b64decode(base64str)
    return bytearray([(b << 1 & 255) | (b >> 7) for b in tmp]).decode("utf8")


def sqlyog_encode(text):
    tmp = text.encode("utf8")
    return base64.b64encode(bytearray([(b >> 1) | ((b & 1) << 7) for b in tmp])).decode("utf8")


class Module17(ttk.Frame):
    def __init__(self, tab_control, xyroot):
        super().__init__(tab_control)
        self.tab_control = tab_control
        self.xyroot = xyroot
        
        self.db_connections = []
        self.sql_templates = []
        self.current_template = None
        self.selected_db_config = None
        self.current_result_id = None
        
        self.setup_styles()
        self._create_main_tab()
        
        self.load_db_connections()
        self.load_sql_templates()
        
    def setup_styles(self):
        style = ttk.Style()
        
        style.configure('Title.TLabel', font=('Microsoft YaHei UI', 12, 'bold'), foreground='#2c3e50')
        style.configure('Header.TLabel', font=('Microsoft YaHei UI', 10, 'bold'), foreground='#34495e')
        style.configure('Info.TLabel', font=('Microsoft YaHei UI', 9), foreground='#7f8c8d')
        
        style.configure('Action.TButton', font=('Microsoft YaHei UI', 10), padding=(15, 8))
        style.configure('Primary.TButton', font=('Microsoft YaHei UI', 10, 'bold'), padding=(20, 10))
        style.configure('SQL.TButton', font=('Microsoft YaHei UI', 9), padding=(10, 5))
        
        style.configure('Result.Treeview', 
                       font=('Microsoft YaHei UI', 9),
                       rowheight=28)
        style.configure('Result.Treeview.Heading', 
                       font=('Microsoft YaHei UI', 9, 'bold'))
        
    def center_window(self, window, parent=None):
        window.update_idletasks()
        width = window.winfo_width()
        height = window.winfo_height()
        if parent:
            x = (parent.winfo_width() // 2) - (width // 2) + parent.winfo_x()
            y = (parent.winfo_height() // 2) - (height // 2) + parent.winfo_y()
        else:
            x = (window.winfo_screenwidth() // 2) - (width // 2)
            y = (window.winfo_screenheight() // 2) - (height // 2)
        window.geometry(f'+{x}+{y}')
        
    def _create_main_tab(self):
        main_frame = ttk.Frame(self.tab_control)
        self.tab_control.add(main_frame, text='SQL查询工具')
        
        main_frame.columnconfigure(0, weight=1)
        main_frame.rowconfigure(0, weight=1)
        
        self.sub_notebook = ttk.Notebook(main_frame)
        self.sub_notebook.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        self._create_query_tab()
        self._create_db_manager_tab()
        self._create_template_manager_tab()
        
    def _create_query_tab(self):
        query_frame = ttk.Frame(self.sub_notebook, padding="10")
        self.sub_notebook.add(query_frame, text='SQL查询')
        
        query_frame.columnconfigure(0, weight=1)
        query_frame.rowconfigure(1, weight=1)
        
        self._setup_simple_query_panel(query_frame)
        
    def _setup_top_panel(self, parent):
        top_frame = ttk.Frame(parent)
        top_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 10))
        top_frame.columnconfigure(1, weight=1)
        
        left_frame = ttk.Frame(top_frame)
        left_frame.grid(row=0, column=0, sticky=tk.W)
        
        ttk.Label(left_frame, text="选择目标数据库:", style='Header.TLabel').grid(row=0, column=0, sticky=tk.W, padx=(0, 10))
        
        self.db_var = tk.StringVar()
        self.db_combo = ttk.Combobox(left_frame, textvariable=self.db_var, width=40, state='normal')
        self.db_combo.grid(row=0, column=1, sticky=tk.W, padx=(0, 10))
        self.db_combo.bind('<<ComboboxSelected>>', self.on_db_selected)
        self.db_combo.bind('<KeyRelease>', self.on_db_filter)
        
        btn_frame = ttk.Frame(left_frame)
        btn_frame.grid(row=0, column=2, padx=5)
        ttk.Button(btn_frame, text="刷新连接", command=self.load_db_connections, style='Action.TButton').grid(row=0, column=0, padx=2)
        ttk.Button(btn_frame, text="刷新模板", command=self.load_sql_templates, style='Action.TButton').grid(row=0, column=1, padx=2)
        ttk.Button(btn_frame, text="执行查询", command=self.execute_query, style='Primary.TButton').grid(row=0, column=2, padx=2)
        ttk.Button(btn_frame, text="查看结果", command=self.view_results, style='Action.TButton').grid(row=0, column=3, padx=2)
        
        self._setup_template_panel(parent)
        
    def _setup_middle_panel(self, parent):
        middle_frame = ttk.Frame(parent)
        middle_frame.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N), pady=(0, 10))
        middle_frame.columnconfigure(0, weight=1)
        
        self._setup_sql_panel(middle_frame)
        
    def _setup_template_panel(self, parent):
        template_frame = ttk.Frame(parent)
        template_frame.grid(row=0, column=1, rowspan=4, sticky=(tk.N, tk.S), padx=(10, 0))
        template_frame.rowconfigure(1, weight=1)
        template_frame.columnconfigure(0, weight=1)
        
        header_frame = ttk.Frame(template_frame)
        header_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 5))
        
        ttk.Label(header_frame, text="预设查询模板", style='Header.TLabel').grid(row=0, column=0, sticky=tk.W, padx=(0, 5))
        
        self.template_search_var = tk.StringVar()
        self.template_search_var.trace('w', self.on_template_search)
        search_entry = ttk.Entry(header_frame, textvariable=self.template_search_var, width=10)
        search_entry.grid(row=0, column=1, sticky=tk.W, padx=(0, 3))
        
        ttk.Button(header_frame, text="重置", command=self.reset_template_search, width=4).grid(row=0, column=2)
        
        template_list_frame = ttk.Frame(template_frame)
        template_list_frame.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        template_list_frame.rowconfigure(0, weight=1)
        template_list_frame.columnconfigure(0, weight=1)
        
        self.template_canvas = tk.Canvas(template_list_frame, highlightthickness=0, width=160)
        self.template_scrollbar = ttk.Scrollbar(template_list_frame, orient="vertical", command=self.template_canvas.yview)
        self.template_inner_frame = ttk.Frame(self.template_canvas)
        
        self.template_canvas.configure(yscrollcommand=self.template_scrollbar.set)
        
        self.template_scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        self.template_canvas.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        self.canvas_window = self.template_canvas.create_window((0, 0), window=self.template_inner_frame, anchor="nw")
        
        self.template_inner_frame.bind("<Configure>", self._on_template_frame_configure)
        self.template_canvas.bind("<Configure>", self._on_canvas_configure)
        
        self.template_canvas.bind('<Enter>', self._bind_mousewheel)
        self.template_canvas.bind('<Leave>', self._unbind_mousewheel)
        
        self.template_buttons = []
        
    def _on_template_frame_configure(self, event):
        self.template_canvas.configure(scrollregion=self.template_canvas.bbox("all"))
        
    def _on_canvas_configure(self, event):
        self.template_canvas.itemconfig(self.canvas_window, width=event.width)
        
    def _bind_mousewheel(self, event):
        self.template_canvas.bind_all("<MouseWheel>", self._on_mousewheel)
        
    def _unbind_mousewheel(self, event):
        self.template_canvas.unbind_all("<MouseWheel>")
        
    def _on_mousewheel(self, event):
        self.template_canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        
    def _setup_sql_panel(self, parent):
        sql_frame = ttk.Frame(parent)
        sql_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N))
        sql_frame.columnconfigure(0, weight=1)
        
        sql_text_frame = ttk.Frame(sql_frame)
        sql_text_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 10))
        sql_text_frame.columnconfigure(0, weight=1)
        
        #ttk.Label(sql_text_frame, text="SQL与参数配置", style='Header.TLabel').grid(row=0, column=0, sticky=tk.W)
        
        self.sql_text = scrolledtext.ScrolledText(sql_text_frame, wrap=tk.WORD,
                                                   font=('Consolas', 10), state=tk.DISABLED, height=5)
        self.sql_text.grid(row=1, column=0, sticky=(tk.W, tk.E))
        
        param_frame = ttk.Frame(sql_frame)
        param_frame.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=(0, 10))
        param_frame.columnconfigure(1, weight=1)
        
        self.param_frame = param_frame
        self.param_entries = []
        
        ttk.Label(param_frame, text="参数输入", style='Header.TLabel').grid(row=0, column=0, columnspan=2, sticky=tk.W, pady=(0, 5))
        ttk.Label(param_frame, text="请选择预设SQL模板", style='Info.TLabel').grid(row=1, column=0, columnspan=2)
        
    def _setup_simple_query_panel(self, parent):
        main_frame = ttk.Frame(parent)
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 10))
        main_frame.columnconfigure(0, weight=1)
        main_frame.rowconfigure(1, weight=1)
        
        ttk.Label(main_frame, text="选择数据库连接:", style='Header.TLabel').grid(row=0, column=0, sticky=tk.W, padx=(0, 10), pady=(0, 10))
        
        control_frame = ttk.Frame(main_frame)
        control_frame.grid(row=1, column=0, sticky=(tk.W, tk.E))
        
        self.db_var = tk.StringVar()
        self.db_combo = ttk.Combobox(control_frame, textvariable=self.db_var, width=60, state='readonly')
        self.db_combo.grid(row=0, column=0, sticky=tk.W, padx=(0, 10))
        self.db_combo.bind('<<ComboboxSelected>>', self.on_db_selected)
        
        btn_frame = ttk.Frame(control_frame)
        btn_frame.grid(row=0, column=1, padx=5)
        
        ttk.Button(btn_frame, text="刷新连接", command=self.load_db_connections, style='Action.TButton').grid(row=0, column=0, padx=2)
        ttk.Button(btn_frame, text="Link", command=self.open_web_interface, style='Primary.TButton').grid(row=0, column=1, padx=2)
        
        self.status_var = tk.StringVar(value="就绪")
        status_bar = ttk.Label(parent, textvariable=self.status_var, style='Info.TLabel', relief=tk.SUNKEN, anchor=tk.W)
        status_bar.grid(row=2, column=0, sticky=(tk.W, tk.E), pady=(10, 0))
        
    def _create_db_manager_tab(self):
        self.db_manager_frame = ttk.Frame(self.sub_notebook, padding="10")
        self.sub_notebook.add(self.db_manager_frame, text='数据库连接管理')
        
        self.db_manager_frame.columnconfigure(0, weight=1)
        self.db_manager_frame.rowconfigure(0, weight=1)
        
        tree_frame = ttk.Frame(self.db_manager_frame)
        tree_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        tree_frame.columnconfigure(0, weight=1)
        tree_frame.rowconfigure(0, weight=1)
        
        columns = ('name', 'host', 'port', 'user', 'database', 'ssh')
        self.db_tree = ttk.Treeview(tree_frame, columns=columns, show='headings', selectmode='browse')
        
        self.db_tree.heading('name', text='名称')
        self.db_tree.heading('host', text='主机')
        self.db_tree.heading('port', text='端口')
        self.db_tree.heading('user', text='用户')
        self.db_tree.heading('database', text='数据库')
        self.db_tree.heading('ssh', text='SSH')
        
        self.db_tree.column('name', width=120)
        self.db_tree.column('host', width=120)
        self.db_tree.column('port', width=60)
        self.db_tree.column('user', width=80)
        self.db_tree.column('database', width=100)
        self.db_tree.column('ssh', width=60)
        
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self.db_tree.yview)
        hsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.db_tree.xview)
        self.db_tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        
        self.db_tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        vsb.grid(row=0, column=1, sticky=(tk.N, tk.S))
        hsb.grid(row=1, column=0, sticky=(tk.W, tk.E))
        
        self.db_tree.bind('<<TreeviewSelect>>', self.on_db_tree_select)
        self.db_tree.bind('<Double-1>', self.edit_db_connection)
        
        btn_frame = ttk.Frame(self.db_manager_frame)
        btn_frame.grid(row=1, column=0, pady=10)
        
        btn_configs = [
            ("新增连接", self.add_db_connection),
            ("编辑连接", self.edit_db_connection),
            ("删除连接", self.delete_db_connection),
            ("导入.sycs", self.import_sycs),
            ("测试连接", self.test_db_connection),
            ("刷新列表", self.load_db_connections_to_tree)
        ]
        
        for col_idx, (btn_text, cmd) in enumerate(btn_configs):
            ttk.Button(btn_frame, text=btn_text, command=cmd, style='Action.TButton').grid(row=0, column=col_idx, padx=5)
        
        self.selected_db_connection = None
        
    def _create_template_manager_tab(self):
        self.template_manager_frame = ttk.Frame(self.sub_notebook, padding="10")
        self.sub_notebook.add(self.template_manager_frame, text='SQL模板管理')
        
        self.template_manager_frame.columnconfigure(0, weight=1)
        self.template_manager_frame.rowconfigure(0, weight=1)
        
        tree_frame = ttk.Frame(self.template_manager_frame)
        tree_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        tree_frame.columnconfigure(0, weight=1)
        tree_frame.rowconfigure(0, weight=1)
        
        columns = ('name', 'description')
        self.tpl_tree = ttk.Treeview(tree_frame, columns=columns, show='headings', selectmode='browse')
        
        self.tpl_tree.heading('name', text='名称')
        self.tpl_tree.heading('description', text='描述')
        
        self.tpl_tree.column('name', width=200)
        self.tpl_tree.column('description', width=400)
        
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tpl_tree.yview)
        hsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.tpl_tree.xview)
        self.tpl_tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        
        self.tpl_tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        vsb.grid(row=0, column=1, sticky=(tk.N, tk.S))
        hsb.grid(row=1, column=0, sticky=(tk.W, tk.E))
        
        self.tpl_tree.bind('<<TreeviewSelect>>', self.on_tpl_tree_select)
        self.tpl_tree.bind('<Double-1>', self.edit_sql_template)
        
        btn_frame = ttk.Frame(self.template_manager_frame)
        btn_frame.grid(row=1, column=0, pady=10)
        
        btn_configs = [
            ("新增模板", self.add_sql_template),
            ("编辑模板", self.edit_sql_template),
            ("删除模板", self.delete_sql_template),
            ("刷新列表", self.load_sql_templates_to_tree)
        ]
        
        for col_idx, (btn_text, cmd) in enumerate(btn_configs):
            ttk.Button(btn_frame, text=btn_text, command=cmd, style='Action.TButton').grid(row=0, column=col_idx, padx=5)
        
        self.selected_sql_template = None
        
    def open_web_interface(self):
        if not self.selected_db_config:
            self.show_dialog("警告", "请先选择目标数据库")
            return
        
        result_viewer.start_server()
        url = result_viewer.open_web_console(self.selected_db_config)
        self.status_var.set(f"已在浏览器中打开查询器: {url}")
    
    def load_db_connections(self):
        try:
            sql = "SELECT * FROM sqlyog_connections_test ORDER BY Name"
            results = execute_sql('mysql.xjjhhb01', sql)
            self.db_connections = []
            if results:
                for conn in results:
                    self.db_connections.append({
                        'id': conn.get('Connection_id'),
                        'name': conn.get('Name'),
                        'host': conn.get('Host'),
                        'port': conn.get('Port', 3306),
                        'user': conn.get('User'),
                        'password': conn.get('Password'),
                        'password_encrypted': 1,
                        'database': conn.get('Database', ''),
                        'ssh_enabled': conn.get('SSH', 0),
                        'ssh_host': conn.get('SshHost', ''),
                        'ssh_port': conn.get('SshPort', 22),
                        'ssh_user': conn.get('SshUser', ''),
                        'ssh_password': conn.get('SshPwd', ''),
                        'ssh_password_encrypted': 1,
                    })
            self._update_db_combo()
            self.load_db_connections_to_tree()
        except Exception as e:
            self.status_var.set(f"加载数据库连接失败: {str(e)}")
            self.db_connections = []
            
    def load_db_connections_to_tree(self):
        for item in self.db_tree.get_children():
            self.db_tree.delete(item)
            
        try:
            for conn in self.db_connections:
                ssh_text = "是" if conn.get('ssh_enabled', 0) == 1 else "否"
                self.db_tree.insert('', 'end', values=(
                    conn.get('name', ''),
                    conn.get('host', ''),
                    conn.get('port', 3306),
                    conn.get('user', ''),
                    conn.get('database', ''),
                    ssh_text
                ), tags=(conn.get('id', ''),))
        except Exception as e:
            self.show_dialog("错误", f"加载连接失败: {str(e)}")
            
    def _update_db_combo(self, filter_text=''):
        db_names = []
        for conn in self.db_connections:
            name = conn.get('name', '')
            if filter_text:
                if filter_text.lower() in name.lower():
                    db_names.append(name)
            else:
                db_names.append(name)
        self.db_combo['values'] = db_names
        
    def on_db_selected(self, event):
        selected_name = self.db_var.get()
        for conn in self.db_connections:
            if conn.get('name') == selected_name:
                self.selected_db_config = conn
                self.status_var.set(f"已选择数据库: {selected_name}")
                break
                
    def on_db_filter(self, event):
        filter_text = self.db_var.get()
        self._update_db_combo(filter_text)
        
    def on_db_tree_select(self, event):
        selection = self.db_tree.selection()
        if selection:
            item = selection[0]
            conn_id = self.db_tree.item(item, 'tags')[0] if self.db_tree.item(item, 'tags') else None
            self.selected_db_connection = conn_id
            
    def load_sql_templates(self):
        try:
            sql = "SELECT * FROM sql_query_templates ORDER BY name"
            results = execute_sql('mysql.xjjhhb01', sql)
            self.sql_templates = results if results else []
            self._render_template_buttons()
            self.load_sql_templates_to_tree()
        except Exception as e:
            self.status_var.set(f"加载SQL模板失败: {str(e)}")
            self.sql_templates = []
            
    def load_sql_templates_to_tree(self):
        for item in self.tpl_tree.get_children():
            self.tpl_tree.delete(item)
            
        try:
            for template in self.sql_templates:
                self.tpl_tree.insert('', 'end', values=(
                    template.get('name', ''),
                    template.get('description', '')
                ), tags=(template.get('id', ''),))
        except Exception as e:
            self.show_dialog("错误", f"加载模板失败: {str(e)}")
            
    def on_tpl_tree_select(self, event):
        selection = self.tpl_tree.selection()
        if selection:
            item = selection[0]
            template_id = self.tpl_tree.item(item, 'tags')[0] if self.tpl_tree.item(item, 'tags') else None
            self.selected_sql_template = template_id
            
    def _render_template_buttons(self, filter_text=''):
        for widget in self.template_inner_frame.winfo_children():
            widget.destroy()
        self.template_buttons = []
        
        for template in self.sql_templates:
            name = template.get('name', '')
            if filter_text:
                if filter_text.lower() not in name.lower():
                    continue
                    
            btn = ttk.Button(self.template_inner_frame, text=name, 
                           command=lambda t=template: self.on_template_selected(t),
                           style='SQL.TButton', width=20)
            btn.pack(fill=tk.X, pady=2)
            
            self._create_tooltip(btn, name)
            self.template_buttons.append(btn)
            
    def _create_tooltip(self, widget, text):
        def show_tooltip(event):
            tooltip = tk.Toplevel(widget)
            tooltip.wm_overrideredirect(True)
            tooltip.wm_geometry(f"+{event.x_root + 10}+{event.y_root + 10}")
            
            label = ttk.Label(tooltip, text=text, background="#ffffe0", 
                            relief="solid", borderwidth=1, padding=(5, 2))
            label.pack()
            
            widget.tooltip = tooltip
            
        def hide_tooltip(event):
            if hasattr(widget, 'tooltip'):
                widget.tooltip.destroy()
                del widget.tooltip
                
        widget.bind('<Enter>', show_tooltip)
        widget.bind('<Leave>', hide_tooltip)
        
    def on_template_selected(self, template):
        self.current_template = template
        sql_content = template.get('sql_content', '')
        
        self.sql_text.config(state=tk.NORMAL)
        self.sql_text.delete(1.0, tk.END)
        self.sql_text.insert(1.0, sql_content)
        self.sql_text.config(state=tk.DISABLED)
        
        self._parse_and_render_params(sql_content)
        self.status_var.set(f"已加载模板: {template.get('name', '')}")
        
    def _parse_and_render_params(self, sql_content):
        for widget in self.param_frame.winfo_children():
            widget.destroy()
        self.param_entries = []
        
        pattern = r'\$P\{([^}]+)\}'
        params = re.findall(pattern, sql_content)
        
        if not params:
            ttk.Label(self.param_frame, text="无需参数", style='Info.TLabel').grid(row=2, column=0, columnspan=2)
            return
            
        for i, param in enumerate(params):
            ttk.Label(self.param_frame, text=f"参数{i+1}: {param}:", style='Header.TLabel').grid(row=i+2, column=0, sticky=tk.W, padx=(0, 5), pady=2)
            
            entry = ttk.Entry(self.param_frame, width=40)
            entry.grid(row=i+2, column=1, sticky=(tk.W, tk.E), pady=2)
            
            self.param_entries.append((param, entry))
            
    def on_template_search(self, *args):
        filter_text = self.template_search_var.get()
        self._render_template_buttons(filter_text)
        
    def reset_template_search(self):
        self.template_search_var.set('')
        self._render_template_buttons()
        
    def execute_query(self):
        if not self.selected_db_config:
            self.show_dialog("警告", "请先选择目标数据库")
            return
            
        if not self.current_template:
            self.show_dialog("警告", "请先选择预设SQL模板")
            return
            
        sql_content = self.current_template.get('sql_content', '')
        if not sql_content:
            self.show_dialog("警告", "SQL内容为空")
            return
        
        param_values = {}
        param_order = []
        for param, entry in self.param_entries:
            value = entry.get()
            if not value:
                self.show_dialog("警告", f"请填写参数: {param}")
                return
            param_values[param] = value
            param_order.append(param)
        
        final_sql = sql_content
        args_list = []
        for param in param_order:
            placeholder = f'$P{{{param}}}'
            if placeholder in final_sql:
                final_sql = final_sql.replace(placeholder, '%s', 1)
                args_list.append(param_values[param])
        
        if not final_sql.strip().lower().startswith('select'):
            self.show_dialog("警告", "只允许执行SELECT查询")
            return
        
        print(f"[SQL执行] {final_sql}")
        print(f"[参数] {tuple(args_list)}")
            
        self.status_var.set("正在执行查询...")
        start_time = time.time()
        
        try:
            table_names = self.current_template.get('table_names', '')
            results = self._execute_sql_with_connection(final_sql, tuple(args_list), table_names)
            elapsed_time = time.time() - start_time
            
            if results and results.get('rows'):
                self._display_results_web(results, final_sql, elapsed_time)
                row_count = len(results['rows'])
                self.status_var.set(f"查询完成，返回 {row_count} 条记录，耗时 {elapsed_time:.2f} 秒")
            else:
                self.status_var.set(f"查询完成，无结果，耗时 {elapsed_time:.2f} 秒")
                self.show_dialog("提示", "查询完成，无结果")
                
        except Exception as e:
            self.status_var.set(f"查询失败: {str(e)}")
            print(f"查询失败: {str(e)}")
            self.show_dialog("错误", f"查询执行失败:\n{str(e)}")
    
    def _execute_sql_with_connection(self, sql, args=None, table_names=None):
        conn_config = self.selected_db_config
        
        host = conn_config.get('host', '')
        port = conn_config.get('port', 3306)
        user = conn_config.get('user', '')
        password = conn_config.get('password', '')
        database = conn_config.get('database', '')
        ssh_enabled = conn_config.get('ssh_enabled', 0)
        
        if conn_config.get('password_encrypted', 0) == 1:
            password = sqlyog_decode(password)
            
        if ssh_enabled == 1:
            return self._execute_via_ssh(sql, conn_config, args, table_names)
        else:
            return self._execute_direct(sql, host, port, user, password, database, args, table_names)
            
    def _execute_direct(self, sql, host, port, user, password, database, args=None, table_names=None):
        with pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=10
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, args or ())
                rows = cursor.fetchall()
                columns = []
                if cursor.description:
                    if table_names:
                        tables = [t.strip() for t in table_names.split(',') if t.strip()]
                    else:
                        tables = self._extract_table_names(sql)
                    
                    alias_to_table = self._extract_table_alias_mapping(sql)
                    column_types = self._get_column_types_from_schema(conn, database, tables)
                    
                    print(f"[调试] 表别名映射: {alias_to_table}")
                    
                    for col in cursor.description:
                        col_name = col[0]
                        col_name_lower = col_name.lower()
                        col_type = column_types.get(col_name_lower, self._get_column_type_name(col[1]))
                        col_table = column_types.get(f'_table_{col_name_lower}', '')
                        
                        if '.' in col_name:
                            alias = col_name.split('.')[0].lower()
                            if alias in alias_to_table:
                                col_table = alias_to_table[alias]
                        
                        print(f"[调试] 列: {col_name} -> 类型: {col_type}, 表: {col_table}")
                        
                        columns.append({
                            'name': col_name,
                            'type': col_type,
                            'table': col_table
                        })
                return {'rows': rows, 'columns': columns}
    
    def _extract_table_alias_mapping(self, sql):
        import re
        sql_lower = sql.lower()
        alias_map = {}
        
        from_match = re.search(r'from\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+(?:as\s+)?([a-zA-Z_][a-zA-Z0-9_]*)', sql_lower)
        if from_match:
            alias_map[from_match.group(2)] = from_match.group(1)
        
        join_matches = re.findall(r'(?:left|right|inner|outer|cross|natural)?\s*join\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+(?:as\s+)?([a-zA-Z_][a-zA-Z0-9_]*)', sql_lower)
        for match in join_matches:
            alias_map[match[1]] = match[0]
        
        return alias_map
    
    def _extract_table_names(self, sql):
        import re
        sql_lower = sql.lower()
        tables = []
        
        from_match = re.search(r'from\s+([a-zA-Z_][a-zA-Z0-9_]*)', sql_lower)
        if from_match:
            tables.append(from_match.group(1))
        
        join_matches = re.findall(r'(?:left|right|inner|outer|cross|natural)?\s*join\s+([a-zA-Z_][a-zA-Z0-9_]*)', sql_lower)
        tables.extend(join_matches)
        
        return list(set(tables))
    
    def _get_column_types_from_schema(self, conn, database, table_names):
        column_types = {}
        
        if not table_names:
            return column_types
        
        try:
            with conn.cursor() as cursor:
                table_list = "','".join(table_names)
                query = f"""SELECT COLUMN_NAME, DATA_TYPE, TABLE_NAME 
                           FROM INFORMATION_SCHEMA.COLUMNS 
                           WHERE TABLE_SCHEMA = '{database}' 
                           AND TABLE_NAME IN ('{table_list}')"""
                cursor.execute(query)
                results = cursor.fetchall()
                
                for row in results:
                    col_name_lower = row['COLUMN_NAME'].lower()
                    table_name = row['TABLE_NAME']
                    data_type = row['DATA_TYPE'].upper()
                    
                    key = f"{table_name.lower()}.{col_name_lower}"
                    column_types[key] = data_type
                    
                    if col_name_lower not in column_types:
                        column_types[col_name_lower] = data_type
                        column_types[f'_table_{col_name_lower}'] = table_name
                    else:
                        if f'_tables_{col_name_lower}' not in column_types:
                            column_types[f'_tables_{col_name_lower}'] = [column_types.get(f'_table_{col_name_lower}', '')]
                        column_types[f'_tables_{col_name_lower}'].append(table_name)
                        
        except Exception as e:
            print(f"获取字段类型失败: {e}")
        
        return column_types
    
    def _get_column_type_name(self, type_code):
        type_map = {
            0: 'DECIMAL',
            1: 'TINY',
            2: 'SHORT',
            3: 'LONG',
            4: 'FLOAT',
            5: 'DOUBLE',
            6: 'NULL',
            7: 'TIMESTAMP',
            8: 'LONGLONG',
            9: 'INT24',
            10: 'DATE',
            11: 'TIME',
            12: 'DATETIME',
            13: 'YEAR',
            14: 'NEWDATE',
            15: 'VARCHAR',
            16: 'BIT',
            245: 'JSON',
            246: 'NEWDECIMAL',
            247: 'ENUM',
            248: 'SET',
            249: 'TINYBLOB',
            250: 'MEDIUMBLOB',
            251: 'LONGBLOB',
            252: 'BLOB',
            253: 'VARCHAR',
            254: 'CHAR',
            255: 'GEOMETRY',
        }
        return type_map.get(type_code, 'UNKNOWN')
                
    def _execute_via_ssh(self, sql, conn_config, args=None, table_names=None):
        ssh_host = conn_config.get('ssh_host', '')
        ssh_port = conn_config.get('ssh_port', 22)
        ssh_user = conn_config.get('ssh_user', '')
        ssh_password = conn_config.get('ssh_password', '')
        
        if conn_config.get('ssh_password_encrypted', 0) == 1:
            ssh_password = sqlyog_decode(ssh_password)
            
        host = conn_config.get('host', '')
        port = conn_config.get('port', 3306)
        user = conn_config.get('user', '')
        password = conn_config.get('password', '')
        database = conn_config.get('database', '')
        
        if conn_config.get('password_encrypted', 0) == 1:
            password = sqlyog_decode(password)
            
        is_win7 = platform.system() == 'Windows' and platform.release() == '7'
        
        if is_win7:
            with self._plink_tunnel(ssh_host, ssh_port, ssh_user, ssh_password, host, port) as local_port:
                return self._execute_direct(sql, '127.0.0.1', local_port, user, password, database, args, table_names)
        else:
            from sshtunnel import SSHTunnelForwarder
            with SSHTunnelForwarder(
                (ssh_host, ssh_port),
                ssh_username=ssh_user,
                ssh_password=ssh_password,
                remote_bind_address=(host, port)
            ) as tunnel:
                return self._execute_direct(sql, '127.0.0.1', tunnel.local_bind_port, user, password, database, args, table_names)
                
    @contextmanager
    def _plink_tunnel(self, ssh_host, ssh_port, ssh_user, ssh_password, db_host, db_port):
        local_port = self._find_free_port()
        plink_process = None
        
        try:
            plink_cmd = [
                'plink.exe',
                '-ssh',
                '-L', f'{local_port}:{db_host}:{db_port}',
                '-P', str(ssh_port),
                '-l', ssh_user,
                '-pw', ssh_password,
                '-N',
                '-batch',
                ssh_host
            ]
            
            plink_process = subprocess.Popen(
                plink_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                stdin=subprocess.PIPE
            )
            
            if not self._wait_for_port('127.0.0.1', local_port, timeout=10):
                raise ConnectionError("无法建立SSH隧道")
                
            yield local_port
            
        finally:
            if plink_process and plink_process.poll() is None:
                plink_process.terminate()
                try:
                    plink_process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    plink_process.kill()
                    
    def _find_free_port(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('', 0))
            return s.getsockname()[1]
            
    def _wait_for_port(self, host, port, timeout=5):
        start_time = time.time()
        while time.time() - start_time < timeout:
            if self._is_port_open(host, port):
                return True
            time.sleep(0.1)
        return False
        
    def _is_port_open(self, host, port):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(0.5)
                s.connect((host, port))
                return True
        except:
            return False
    
    def _display_results_web(self, results, sql, elapsed_time):
        if not results:
            return
            
        rows_data = results.get('rows', [])
        columns_info = results.get('columns', [])
        
        columns = [col['name'] for col in columns_info]
        column_types = {col['name']: col['type'] for col in columns_info}
        
        processed_rows = []
        for row in rows_data:
            row_data = []
            for col in columns:
                value = row.get(col)
                if value is None:
                    row_data.append(None)
                elif column_types.get(col) in ('BLOB', 'TINYBLOB', 'MEDIUMBLOB', 'LONGBLOB'):
                    if isinstance(value, bytes):
                        import base64
                        try:
                            b64_data = base64.b64encode(value).decode('utf-8')
                            row_data.append(f'data:application/octet-stream;base64,{b64_data}')
                        except:
                            row_data.append('[BLOB]')
                    else:
                        row_data.append(str(value))
                elif isinstance(value, bytes):
                    try:
                        row_data.append(value.decode('utf-8'))
                    except:
                        import base64
                        b64_data = base64.b64encode(value).decode('utf-8')
                        row_data.append(f'[BINARY:{len(value)}bytes]')
                else:
                    row_data.append(str(value) if value is not None else None)
            processed_rows.append(row_data)
        
        query_info = {
            'sql': sql,
            'elapsed_time': elapsed_time,
            'row_count': len(rows_data),
            'database': self.selected_db_config.get('name', '') if self.selected_db_config else '',
            'column_types': column_types
        }
        
        self.current_result_id = result_viewer.add_result(columns, processed_rows, query_info)
        result_viewer.start_server()
        
        url = result_viewer.open_browser(self.current_result_id)
        self.status_var.set(f"已在浏览器中打开结果: {url}")
        
    def view_results(self):
        if not hasattr(self, 'current_result_id') or self.current_result_id is None:
            self.show_dialog("提示", "没有可查看的结果，请先执行查询")
            return
            
        url = result_viewer.open_browser(self.current_result_id)
        self.status_var.set(f"已在浏览器中打开结果: {url}")
            
    def show_dialog(self, title, message):
        dialog = CustomDialog(
            self.xyroot,
            title=title,
            message=message,
            ok_text="确认",
            cancel_text=""
        )
        self.tab_control.wait_window(dialog.top)
        
    def add_db_connection(self):
        self._show_db_connection_dialog()
        
    def edit_db_connection(self, event=None):
        if not self.selected_db_connection:
            self.show_dialog("提示", "请先选择要编辑的连接")
            return
            
        sql = "SELECT * FROM sqlyog_connections_test WHERE Connection_id = %s"
        results = execute_sql('mysql.xjjhhb01', sql, (self.selected_db_connection,))
        
        if results:
            self._show_db_connection_dialog(results[0])
            
    def _show_db_connection_dialog(self, conn_data=None):
        dialog = tk.Toplevel(self.xyroot)
        dialog.title("编辑连接" if conn_data else "新增连接")
        dialog.attributes('-toolwindow', True)
        dialog.transient(self.xyroot)
        dialog.grab_set()
        
        dialog.saved = False
        
        main_frame = tk.Frame(dialog)
        main_frame.pack(padx=10, pady=10)
        
        fields = [
            ('name', '名称:', ''),
            ('host', '主机:', ''),
            ('port', '端口:', '3306'),
            ('user', '用户名:', ''),
            ('password', '密码:', ''),
            ('database', '数据库:', ''),
        ]
        
        entries = {}
        
        for i, (key, label, default) in enumerate(fields):
            tk.Label(main_frame, text=label).grid(row=i, column=0, sticky=tk.W, pady=2)
            
            if key == 'password':
                entry = ttk.Entry(main_frame, show='*', width=40)
            else:
                entry = ttk.Entry(main_frame, width=40)
                
            entry.grid(row=i, column=1, sticky=(tk.W, tk.E), pady=2)
            
            if conn_data:
                # 处理sqlyog_connections_test表的字段名
                if key == 'name':
                    value = conn_data.get('Name', '')
                elif key == 'host':
                    value = conn_data.get('Host', '')
                elif key == 'port':
                    value = conn_data.get('Port', 3306)
                elif key == 'user':
                    value = conn_data.get('User', '')
                elif key == 'password':
                    value = conn_data.get('Password', '')
                elif key == 'database':
                    value = conn_data.get('Database', '')
                else:
                    value = conn_data.get(key, '')
                # 所有密码都是加密的
                if key == 'password':
                    try:
                        value = sqlyog_decode(value)
                    except:
                        value = ''
                entry.insert(0, str(value) if value else default)
            else:
                entry.insert(0, default)
                
            entries[key] = entry
            
        ssh_var = tk.IntVar(value=conn_data.get('SSH', 0) if conn_data else 0)
        ssh_frame = tk.Frame(main_frame)
        
        def toggle_ssh():
            state = tk.NORMAL if ssh_var.get() else tk.DISABLED
            for widget in ssh_frame.winfo_children():
                widget.configure(state=state)
                
        tk.Checkbutton(main_frame, text="启用SSH隧道", variable=ssh_var, 
                       command=toggle_ssh).grid(row=len(fields), column=0, columnspan=2, pady=10, sticky=tk.W)
        
        ssh_frame.grid(row=len(fields)+1, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=5)
        
        ssh_fields = [
            ('ssh_host', 'SSH主机:', ''),
            ('ssh_port', 'SSH端口:', '22'),
            ('ssh_user', 'SSH用户:', ''),
            ('ssh_password', 'SSH密码:', ''),
        ]
        
        ssh_entries = {}
        
        for i, (key, label, default) in enumerate(ssh_fields):
            tk.Label(ssh_frame, text=label).grid(row=i, column=0, sticky=tk.W, pady=2)
            
            if key == 'ssh_password':
                entry = ttk.Entry(ssh_frame, show='*', width=40)
            else:
                entry = ttk.Entry(ssh_frame, width=40)
                
            entry.grid(row=i, column=1, sticky=(tk.W, tk.E), pady=2)
            
            if conn_data:
                # 处理sqlyog_connections_test表的SSH字段名
                if key == 'ssh_host':
                    value = conn_data.get('SshHost', '')
                elif key == 'ssh_port':
                    value = conn_data.get('SshPort', 22)
                elif key == 'ssh_user':
                    value = conn_data.get('SshUser', '')
                elif key == 'ssh_password':
                    value = conn_data.get('SshPwd', '')
                else:
                    value = conn_data.get(key, '')
                # 所有密码都是加密的
                if key == 'ssh_password':
                    try:
                        value = sqlyog_decode(value)
                    except:
                        value = ''
                entry.insert(0, str(value) if value else default)
            else:
                entry.insert(0, default)
                
            ssh_entries[key] = entry
            
        if ssh_var.get() == 0:
            for widget in ssh_frame.winfo_children():
                widget.configure(state=tk.DISABLED)
                
        def save_connection():
            name = entries['name'].get().strip()
            host = entries['host'].get().strip()
            port = int(entries['port'].get() or 3306)
            user = entries['user'].get().strip()
            password = entries['password'].get().strip()
            database = entries['database'].get().strip()
            
            if not all([name, host, user, password, database]):
                self.show_dialog("错误", "请填写所有必填项")
                return
                
            ssh_enabled = ssh_var.get()
            ssh_host = ssh_entries['ssh_host'].get().strip() if ssh_enabled else ''
            ssh_port = int(ssh_entries['ssh_port'].get() or 22) if ssh_enabled else 22
            ssh_user = ssh_entries['ssh_user'].get().strip() if ssh_enabled else ''
            ssh_password = ssh_entries['ssh_password'].get().strip() if ssh_enabled else ''
            
            if not self._check_readonly_user(host, port, user, password, database, ssh_enabled, ssh_host, ssh_port, ssh_user, ssh_password):
                self.show_dialog("错误", "该用户不是只读用户，不允许添加")
                return
                
            password_encrypted = sqlyog_encode(password)
            ssh_password_encrypted = sqlyog_encode(ssh_password) if ssh_password else ''
            
            try:
                if conn_data and conn_data.get('Connection_id'):
                    sql = """UPDATE sqlyog_connections_test 
                             SET Name=%s, Host=%s, Port=%s, User=%s, Password=%s, `Database`=%s,
                                 SSH=%s, SshHost=%s, SshPort=%s, SshUser=%s, SshPwd=%s
                             WHERE Connection_id=%s"""
                    execute_sql('mysql.xjjhhb01', sql, (name, host, port, user, password_encrypted, database,
                                                        ssh_enabled, ssh_host, ssh_port, ssh_user, ssh_password_encrypted,
                                                        conn_data['Connection_id']))
                else:
                    sql = """INSERT INTO sqlyog_connections_test 
                             (Name, Host, Port, User, Password, `Database`, SSH, SshHost, SshPort, SshUser, SshPwd)
                             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                    execute_sql('mysql.xjjhhb01', sql, (name, host, port, user, password_encrypted, database,
                                                        ssh_enabled, ssh_host, ssh_port, ssh_user, ssh_password_encrypted))
                                                        
                self.show_dialog("成功", "保存成功")
                dialog.saved = True
                dialog.destroy()
                self.load_db_connections()
                
            except Exception as e:
                self.show_dialog("错误", f"保存失败: {str(e)}")
                
        btn_frame = tk.Frame(main_frame)
        btn_frame.grid(row=len(fields)+2, column=0, columnspan=2, pady=10)
        
        ttk.Button(btn_frame, text="保存", command=save_connection).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="取消", command=dialog.destroy).pack(side=tk.LEFT, padx=5)
        
        self.center_window(dialog, self.xyroot)
        return dialog
        
    def _check_readonly_user(self, host, port, user, password, database, ssh_enabled, ssh_host, ssh_port, ssh_user, ssh_password):
        try:
            if ssh_enabled:
                is_win7 = platform.system() == 'Windows' and platform.release() == '7'
                
                if is_win7:
                    local_port = self._find_free_port()
                    plink_process = subprocess.Popen([
                        'plink.exe', '-ssh', '-L', f'{local_port}:{host}:{port}',
                        '-P', str(ssh_port), '-l', ssh_user, '-pw', ssh_password,
                        '-N', '-batch', ssh_host
                    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    
                    time.sleep(2)
                    
                    conn = pymysql.connect(
                        host='127.0.0.1', port=local_port, user=user, password=password,
                        database=database, connect_timeout=5
                    )
                else:
                    from sshtunnel import SSHTunnelForwarder
                    with SSHTunnelForwarder(
                        (ssh_host, ssh_port), ssh_username=ssh_user, ssh_password=ssh_password,
                        remote_bind_address=(host, port)
                    ) as tunnel:
                        conn = pymysql.connect(
                            host='127.0.0.1', port=tunnel.local_bind_port, user=user, password=password,
                            database=database, connect_timeout=5
                        )
                        return self._verify_readonly(conn)
            else:
                conn = pymysql.connect(
                    host=host, port=port, user=user, password=password,
                    database=database, connect_timeout=5
                )
                
            return self._verify_readonly(conn)
            
        except Exception as e:
            print(f"检查只读用户失败: {str(e)}")
            return False
            
    def _verify_readonly(self, conn):
        try:
            with conn.cursor() as cursor:
                cursor.execute("SHOW GRANTS")
                grants = cursor.fetchall()
                
                for grant in grants:
                    grant_str = grant[0] if isinstance(grant, tuple) else str(grant)
                    grant_lower = grant_str.lower()
                    
                    if any(keyword in grant_lower for keyword in ['insert', 'update', 'delete', 'create', 'drop', 'alter', 'grant option', 'all privileges']):
                        return False
                        
                has_select = False
                for grant in grants:
                    grant_str = grant[0] if isinstance(grant, tuple) else str(grant)
                    if 'select' in grant_str.lower() or 'usage' in grant_str.lower():
                        has_select = True
                        
                return has_select
                
        finally:
            conn.close()
            
    def delete_db_connection(self):
        if not self.selected_db_connection:
            self.show_dialog("提示", "请先选择要删除的连接")
            return
            
        dialog = CustomDialog(
            self.xyroot,
            title="确认删除",
            message="确定要删除该连接吗？",
            ok_text="确认",
            cancel_text="取消"
        )
        self.tab_control.wait_window(dialog.top)
        
        if dialog.result:
            try:
                sql = "DELETE FROM sqlyog_connections_test WHERE Connection_id = %s"
                execute_sql('mysql.xjjhhb01', sql, (self.selected_db_connection,))
                self.show_dialog("成功", "删除成功")
                self.load_db_connections()
            except Exception as e:
                self.show_dialog("错误", f"删除失败: {str(e)}")
                
    def import_sycs(self):
        file_path = filedialog.askopenfilename(filetypes=(("sycs files", "*.sycs"),))
        if not file_path:
            return
            
        try:
            config = configparser.ConfigParser()
            config.optionxform = lambda option: option
            
            for encoding in ['utf-8', 'utf-8-sig', 'gbk']:
                try:
                    with open(file_path, 'r', encoding=encoding) as f:
                        config.read_file(f)
                    break
                except UnicodeDecodeError:
                    continue
                    
            imported_count = 0
            for section in config.sections():
                if not section.startswith('Connection '):
                    continue
                    
                name = config.get(section, 'Name', fallback='')
                host = config.get(section, 'Host', fallback='')
                port = config.getint(section, 'Port', fallback=3306)
                user = config.get(section, 'User', fallback='')
                password_encrypted = config.get(section, 'Password', fallback='')
                database = config.get(section, 'Database', fallback='')
                ssh_enabled = config.getint(section, 'SSH', fallback=0)
                
                try:
                    password = sqlyog_decode(password_encrypted)
                except:
                    password = password_encrypted
                
                ssh_host = config.get(section, 'SshHost', fallback='') if ssh_enabled else ''
                ssh_port = config.getint(section, 'SshPort', fallback=22) if ssh_enabled else 22
                ssh_user = config.get(section, 'SshUser', fallback='') if ssh_enabled else ''
                ssh_password_encrypted = config.get(section, 'SshPwd', fallback='') if ssh_enabled else ''
                
                if ssh_enabled:
                    try:
                        ssh_password = sqlyog_decode(ssh_password_encrypted)
                    except:
                        ssh_password = ssh_password_encrypted
                else:
                    ssh_password = ''
                
                if not all([name, host, user, password]):
                    continue
                    
                check_sql = "SELECT Connection_id FROM sqlyog_connections_test WHERE Name = %s"
                existing = execute_sql('mysql.xjjhhb01', check_sql, (name,))
                
                if existing:
                    continue
                    
                if not database:
                    conn_data = {
                        'Connection_id': None,
                        'Name': name,
                        'Host': host,
                        'Port': port,
                        'User': user,
                        'Password': password,
                        'Database': '',
                        'SSH': ssh_enabled,
                        'SshHost': ssh_host,
                        'SshPort': ssh_port,
                        'SshUser': ssh_user,
                        'SshPwd': ssh_password
                    }
                    dialog = self._show_db_connection_dialog(conn_data)
                    self.tab_control.wait_window(dialog)
                    if dialog.saved:
                        imported_count += 1
                else:
                    sql = """INSERT INTO sqlyog_connections_test 
                             (Name, Host, Port, User, Password, `Database`, SSH, SshHost, SshPort, SshUser, SshPwd)
                             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                    execute_sql('mysql.xjjhhb01', sql, (name, host, port, user, password_encrypted, database,
                                                        ssh_enabled, ssh_host, ssh_port, ssh_user, ssh_password_encrypted))
                    imported_count += 1
                                                    
            self.show_dialog("成功", f"导入完成，共导入 {imported_count} 条连接")
            self.load_db_connections()
            
        except Exception as e:
            self.show_dialog("错误", f"导入失败: {str(e)}")
            
    def test_db_connection(self):
        if not self.selected_db_connection:
            self.show_dialog("提示", "请先选择要测试的连接")
            return
            
        sql = "SELECT * FROM sqlyog_connections_test WHERE Connection_id = %s"
        results = execute_sql('mysql.xjjhhb01', sql, (self.selected_db_connection,))
        
        if not results:
            return
            
        conn_data = results[0]
        
        try:
            host = conn_data.get('host', '')
            port = conn_data.get('port', 3306)
            user = conn_data.get('user', '')
            password = conn_data.get('password', '')
            database = conn_data.get('database', '')
            
            if conn_data.get('password_encrypted', 0) == 1:
                password = sqlyog_decode(password)
                
            if conn_data.get('ssh_enabled', 0) == 1:
                ssh_host = conn_data.get('ssh_host', '')
                ssh_port = conn_data.get('ssh_port', 22)
                ssh_user = conn_data.get('ssh_user', '')
                ssh_password = conn_data.get('ssh_password', '')
                
                if conn_data.get('ssh_password_encrypted', 0) == 1:
                    ssh_password = sqlyog_decode(ssh_password)
                    
                is_win7 = platform.system() == 'Windows' and platform.release() == '7'
                
                if is_win7:
                    local_port = self._find_free_port()
                    plink_process = subprocess.Popen([
                        'plink.exe', '-ssh', '-L', f'{local_port}:{host}:{port}',
                        '-P', str(ssh_port), '-l', ssh_user, '-pw', ssh_password,
                        '-N', '-batch', ssh_host
                    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    
                    time.sleep(2)
                    
                    conn = pymysql.connect(
                        host='127.0.0.1', port=local_port, user=user, password=password,
                        database=database, connect_timeout=5
                    )
                else:
                    from sshtunnel import SSHTunnelForwarder
                    with SSHTunnelForwarder(
                        (ssh_host, ssh_port), ssh_username=ssh_user, ssh_password=ssh_password,
                        remote_bind_address=(host, port)
                    ) as tunnel:
                        conn = pymysql.connect(
                            host='127.0.0.1', port=tunnel.local_bind_port, user=user, password=password,
                            database=database, connect_timeout=5
                        )
                        conn.ping(reconnect=False)
            else:
                conn = pymysql.connect(
                    host=host, port=port, user=user, password=password,
                    database=database, connect_timeout=5
                )
                conn.ping(reconnect=False)
                
            self.show_dialog("成功", "连接测试成功")
            
        except Exception as e:
            self.show_dialog("失败", f"连接测试失败: {str(e)}")
            
    def add_sql_template(self):
        self._show_sql_template_dialog()
        
    def edit_sql_template(self, event=None):
        if not self.selected_sql_template:
            self.show_dialog("提示", "请先选择要编辑的模板")
            return
            
        sql = "SELECT * FROM sql_query_templates WHERE id = %s"
        results = execute_sql('mysql.xjjhhb01', sql, (self.selected_sql_template,))
        
        if results:
            self._show_sql_template_dialog(results[0])
            
    def _show_sql_template_dialog(self, template_data=None):
        dialog = tk.Toplevel(self.xyroot)
        dialog.title("编辑模板" if template_data else "新增模板")
        dialog.attributes('-toolwindow', True)
        dialog.transient(self.xyroot)
        dialog.grab_set()
        
        main_frame = tk.Frame(dialog)
        main_frame.pack(padx=10, pady=10)
        
        main_frame.columnconfigure(1, weight=1)
        main_frame.rowconfigure(3, weight=1)
        
        tk.Label(main_frame, text="名称:").grid(row=0, column=0, sticky=tk.W, pady=2)
        name_entry = ttk.Entry(main_frame, width=50)
        name_entry.grid(row=0, column=1, sticky=(tk.W, tk.E), pady=2)
        
        tk.Label(main_frame, text="描述:").grid(row=1, column=0, sticky=tk.W, pady=2)
        desc_entry = ttk.Entry(main_frame, width=50)
        desc_entry.grid(row=1, column=1, sticky=(tk.W, tk.E), pady=2)
        
        tk.Label(main_frame, text="表名:").grid(row=2, column=0, sticky=tk.W, pady=2)
        table_names_frame = tk.Frame(main_frame)
        table_names_frame.grid(row=2, column=1, sticky=(tk.W, tk.E), pady=2)
        table_names_frame.columnconfigure(0, weight=1)
        
        table_names_entry = ttk.Entry(table_names_frame, width=40)
        table_names_entry.grid(row=0, column=0, sticky=(tk.W, tk.E))
        
        def parse_sql_tables():
            sql_content = sql_text.get(1.0, tk.END).strip()
            tables = self._extract_table_names(sql_content)
            current_tables = table_names_entry.get().strip()
            existing_tables = [t.strip() for t in current_tables.split(',') if t.strip()]
            all_tables = list(set(existing_tables + tables))
            table_names_entry.delete(0, tk.END)
            table_names_entry.insert(0, ','.join(all_tables))
            self.show_dialog("解析结果", f"检测到表: {', '.join(tables) if tables else '未检测到表名'}")
        
        ttk.Button(table_names_frame, text="解析", command=parse_sql_tables, width=6).grid(row=0, column=1, padx=5)
        
        tk.Label(main_frame, text="SQL语句:").grid(row=3, column=0, sticky=tk.NW, pady=2)
        sql_text = scrolledtext.ScrolledText(main_frame, wrap=tk.WORD, height=15, font=('Consolas', 10))
        sql_text.grid(row=3, column=1, sticky=(tk.W, tk.E, tk.N, tk.S), pady=2)
        
        param_hint = tk.Label(main_frame, text="参数格式: $P{参数名}，表名用逗号隔开", font=('Microsoft YaHei UI', 9), foreground='#7f8c8d')
        param_hint.grid(row=4, column=0, columnspan=2, sticky=tk.W, pady=5)
        
        if template_data:
            name_entry.insert(0, template_data.get('name', ''))
            desc_entry.insert(0, template_data.get('description', ''))
            sql_text.insert(1.0, template_data.get('sql_content', ''))
            table_names_entry.insert(0, template_data.get('table_names', ''))
            
        def save_template():
            name = name_entry.get().strip()
            description = desc_entry.get().strip()
            sql_content = sql_text.get(1.0, tk.END).strip()
            table_names = table_names_entry.get().strip()
            
            if not name or not sql_content:
                self.show_dialog("错误", "名称和SQL语句不能为空")
                return
                
            if not sql_content.strip().lower().startswith('select'):
                self.show_dialog("错误", "只允许SELECT查询语句")
                return
                
            try:
                if template_data:
                    sql = "UPDATE sql_query_templates SET name=%s, description=%s, sql_content=%s, table_names=%s WHERE id=%s"
                    execute_sql('mysql.xjjhhb01', sql, (name, description, sql_content, table_names, template_data['id']))
                else:
                    sql = "INSERT INTO sql_query_templates (name, description, sql_content, table_names) VALUES (%s, %s, %s, %s)"
                    execute_sql('mysql.xjjhhb01', sql, (name, description, sql_content, table_names))
                    
                self.show_dialog("成功", "保存成功")
                dialog.destroy()
                self.load_sql_templates()
                
            except Exception as e:
                self.show_dialog("错误", f"保存失败: {str(e)}")
                
        btn_frame = tk.Frame(main_frame)
        btn_frame.grid(row=5, column=0, columnspan=2, pady=10)
        
        ttk.Button(btn_frame, text="保存", command=save_template).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="取消", command=dialog.destroy).pack(side=tk.LEFT, padx=5)
        
        self.center_window(dialog, self.xyroot)
        
    def delete_sql_template(self):
        if not self.selected_sql_template:
            self.show_dialog("提示", "请先选择要删除的模板")
            return
            
        dialog = CustomDialog(
            self.xyroot,
            title="确认删除",
            message="确定要删除该模板吗？",
            ok_text="确认",
            cancel_text="取消"
        )
        self.tab_control.wait_window(dialog.top)
        
        if dialog.result:
            try:
                sql = "DELETE FROM sql_query_templates WHERE id = %s"
                execute_sql('mysql.xjjhhb01', sql, (self.selected_sql_template,))
                self.show_dialog("成功", "删除成功")
                self.load_sql_templates()
            except Exception as e:
                self.show_dialog("错误", f"删除失败: {str(e)}")
