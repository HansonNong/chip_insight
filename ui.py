import logging
from logging.handlers import RotatingFileHandler
import pandas as pd
from nicegui import ui, events

from database import TradeDatabase
from parse_input import TradeImageParser

class ChipInSightUI:
    def __init__(self, port: int = 8080, host: str = "0.0.0.0", log_file: str = "chipinsight.log"):
        self._setup_logging(log_file)
        
        self.parser = TradeImageParser()
        self.db = TradeDatabase()
        self.port = port
        self.host = host

        self.tip_label: ui.label | None = None
        self.table: ui.table | None = None
        self.search_input: ui.input | None = None
        
        self._init_ui()
        # Initial data load
        ui.timer(0.1, self.refresh_table, once=True)

    def _setup_logging(self, log_file: str):
        """Configure logger with file rotation and console output"""
        self.logger = logging.getLogger("ChipInSight")
        self.logger.setLevel(logging.INFO)
        
        if not self.logger.handlers:
            formatter = logging.Formatter(
                '%(asctime)s [%(levelname)s] %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )

            # File handler: 5MB limit, keep 3 backups
            fh = RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=3, encoding='utf-8')
            fh.setFormatter(formatter)
            self.logger.addHandler(fh)

            # Console handler
            ch = logging.StreamHandler()
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

    def log(self, message: str, level: str = "info"):
        """Sync UI label, console, and log file"""
        color_map = {
            "info": "text-blue-500", 
            "success": "text-green-600", 
            "warn": "text-amber-600", 
            "error": "text-red-600"
        }
        
        if self.tip_label:
            self.tip_label.set_text(message)
            self.tip_label.classes(replace=f"text-sm {color_map.get(level, 'text-black')}")
        
        log_mapping = {
            "info": self.logger.info,
            "success": self.logger.info,
            "warn": self.logger.warning,
            "error": self.logger.error
        }
        log_func = log_mapping.get(level, self.logger.info)
        prefix = "[SUCCESS] " if level == "success" else ""
        log_func(f"{prefix}{message}")

    def _init_ui(self):
        ui.page_title("ChipInSight - 股票交易记录管理")
        
        # Header layout
        with ui.header().classes('items-center justify-between bg-slate-800 p-4'):
            with ui.row().classes('items-center'):
                ui.icon('auto_graph', size='lg').classes('text-white')
                ui.label('ChipInSight').classes('text-h5 font-bold text-white')
            
            with ui.row().classes('items-center gap-3'):
                ui.button('刷新数据', icon='refresh', on_click=self.refresh_table).props('flat color=white')
                ui.button('备份并重置', icon='history', on_click=self._confirm_clear).props('flat color=red-300')

        # Main content container
        with ui.column().classes('w-full max-w-6xl mx-auto my-6 p-4'):
            
            # Upload section
            with ui.card().classes('w-full p-6 shadow-sm mb-6'):
                ui.label("同步记录").classes("text-lg font-bold mb-2")
                with ui.row().classes('w-full items-start gap-6'):
                    self.uploader = ui.upload(
                        label="上传股票交易截图 (支持批量)",
                        on_upload=self._parse_trade_image,
                        multiple=True,
                        auto_upload=True
                    ).classes("flex-grow h-32")
                    
                    with ui.column().classes('w-64 p-4 bg-gray-50 rounded'):
                        ui.label("系统状态").classes("text-xs font-bold text-gray-400 uppercase")
                        self.tip_label = ui.label("就绪").classes("text-sm text-gray-600 mt-1")

            # Data table section
            with ui.card().classes('w-full p-6 shadow-sm'):
                with ui.row().classes('w-full items-center justify-between mb-4'):
                    ui.label("流水明细").classes("text-xl font-bold")
                    self.search_input = ui.input(placeholder='搜索股票...').props('outlined dense').classes('w-64')

                self.table = ui.table(
                    columns=[
                        {"name": "time", "label": "时间", "field": "time", "sortable": True, "align": "left"},
                        {"name": "name", "label": "股票", "field": "name", "sortable": True, "align": "left"},
                        {"name": "action", "label": "动作", "field": "action", "align": "center"},
                        {"name": "price", "label": "价格", "field": "price", "sortable": True},
                        {"name": "volume", "label": "数量", "field": "volume", "sortable": True},
                        {"name": "amount", "label": "金额", "field": "amount", "sortable": True},
                    ],
                    rows=[],
                    row_key='id'
                ).classes('w-full border-none shadow-none')

                # Red for Buy, Blue for Sell
                self.table.add_slot('body-cell-action', '''
                    <q-td :props="props">
                        <q-badge :color="props.value === '买入' ? 'red' : (props.value === '卖出' ? 'blue' : 'grey')">
                            {{ props.value }}
                        </q-badge>
                    </q-td>
                ''')
                
                if self.search_input:
                    self.table.bind_filter_from(self.search_input, 'value')

    async def refresh_table(self):
        try:
            df = self.db.get_all_trades()
            if not df.empty and 'time' in df.columns:
                df['time'] = df['time'].astype(str).str.replace('T', ' ')
            
            if self.table:
                self.table.rows = df.to_dict('records')
                self.log(f"已加载 {len(df)} 条记录", "info")

        except Exception:
            self.log("加载失败", "error")
            self.logger.exception("Data refresh failed:")

    async def _parse_trade_image(self, e: events.UploadEventArguments):
        fname = getattr(e, 'name', '未知文件')
        self.log(f"处理中: {fname}", "info")
        
        try:
            img_bytes = await e.file.read()
            df = self.parser.parse(img_bytes)
            
            if df.empty:
                self.log(f"无法识别: {fname}", "warn")
                return

            added = self.db.save_trades(df)
            if added > 0:
                self.log(f"完成: {fname} (+{added})", "success")
                await self.refresh_table()
            else:
                self.log(f"跳过重复: {fname}", "info")
            
            if isinstance(e.sender, ui.upload):
                e.sender.reset()

        except Exception:
            self.log("解析崩溃", "error")
            self.logger.exception(f"OCR processing crashed for {fname}:")

    async def _confirm_clear(self):
        with ui.dialog() as dialog, ui.card().classes('p-6'):
            ui.label('备份并重置数据库？').classes('text-lg font-bold text-red-600')
            ui.label('当前数据将存为 .bak 文件，系统将起用全新的数据库。')
            with ui.row().classes('w-full justify-end gap-2 mt-4'):
                ui.button('取消', on_click=dialog.close).props('outline')
                ui.button('确定', on_click=lambda: self._handle_clear(dialog)).props('color=red')
        dialog.open()

    async def _handle_clear(self, dialog):
        if self.db.clear_all_trades():
            ui.notify('旧数据已备份，新数据库已就绪', type='positive')
            self.log("数据库已备份并重置", "success")
            await self.refresh_table()
        else:
            ui.notify('重置失败', type='negative')
            self.log("重置失败", "error")
        dialog.close()

    def run(self):
        self.logger.info("Application starting...")
        ui.run(title="ChipInSight", port=self.port, host=self.host, reload=False)

if __name__ in {"__main__", "__mp_main__"}:
    ChipInSightUI().run()