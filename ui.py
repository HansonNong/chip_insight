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
        
        self.chip_stock_list: ui.list | None = None
        self.chip_price_table: ui.table | None = None
        self.current_selected_stock: str = ""
        
        self.chip_summary_table: ui.table | None = None
        self.chip_summary_search: ui.input | None = None
        
        self._init_ui()
        ui.timer(0.5, self.refresh_all_data, once=True)

    def _setup_logging(self, log_file: str):
        self.logger = logging.getLogger("ChipInSight")
        self.logger.setLevel(logging.INFO)
        if self.logger.handlers:
            return
        formatter = logging.Formatter(
            '%(asctime)s [%(levelname)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        fh = RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=3, encoding='utf-8')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)

    def log(self, message: str, level: str = "info"):
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
        
        with ui.header().classes('items-center justify-between bg-slate-800 p-4'):
            with ui.row().classes('items-center'):
                ui.icon('auto_graph', size='lg').classes('text-white')
                ui.label('ChipInSight').classes('text-h5 font-bold text-white')
            with ui.row().classes('items-center gap-3'):
                ui.button('刷新数据', icon='refresh', on_click=self.refresh_all_data).props('flat color=white')
                ui.button('备份并重置', icon='history', on_click=self._confirm_clear).props('flat color=red-300')

        with ui.column().classes('w-full max-w-6xl mx-auto my-6 p-4 min-h-[80vh]'):
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

            # Chip price section
            with ui.card().classes('w-full p-6 shadow-sm mb-6'):
                ui.label("筹码价格").classes("text-xl font-bold mb-4")
                with ui.row().classes('w-full gap-4'):
                    with ui.column().classes('w-56 h-[260px] overflow-y-auto border rounded p-2'):
                        ui.label("持仓股票").classes('text-sm font-semibold mb-2')
                        self.chip_stock_list = ui.list().classes('w-full')
                    
                    with ui.column().classes('flex-1'):
                        self.chip_price_table = ui.table(
                            columns=[
                                {"name": "name", "label": "股票", "field": "name", "sortable": True},
                                {"name": "price", "label": "价格", "field": "price", "sortable": True},
                                {"name": "net_volume", "label": "数量", "field": "net_volume", "sortable": True},
                            ],
                            rows=[],
                            row_key="price_key"
                        ).classes('w-full h-[260px]')
                        self.chip_price_table.add_slot('body-cell-net_volume', '''
                            <q-td :props="props">
                                <q-badge :color="props.value > 0 ? 'red' : (props.value < 0 ? 'blue' : 'grey')">
                                    {{ props.value }}
                                </q-badge>
                            </q-td>
                        ''')

            # Chip summary section
            with ui.card().classes('w-full p-6 shadow-sm mb-6'):
                ui.label("筹码统计").classes("text-xl font-bold mb-4")
                self.chip_summary_search = ui.input(placeholder='搜索股票...').props('outlined dense').classes('w-64 mb-4')
                self.chip_summary_search.on('input', self.refresh_chip_summary)
                self.chip_summary_table = ui.table(
                    columns=[
                        {"name": "name", "label": "股票", "field": "name", "sortable": True},
                        {"name": "total_buy", "label": "总买入", "field": "total_buy", "sortable": True},
                        {"name": "total_sell", "label": "总卖出", "field": "total_sell", "sortable": True},
                        {"name": "hold_volume", "label": "当前持仓", "field": "hold_volume", "sortable": True},
                    ],
                    rows=[],
                    row_key="summary_key"
                ).classes('w-full h-[260px]')
                self.chip_summary_table.add_slot('body-cell-hold_volume', '''
                    <q-td :props="props">
                        <q-badge :color="props.value > 0 ? 'green' : (props.value < 0 ? 'red' : 'grey')">
                            {{ props.value }}
                        </q-badge>
                    </q-td>
                ''')

            # Trade history section
            with ui.card().classes('w-full p-6 shadow-sm'):
                ui.label("流水明细").classes("text-xl font-bold mb-4")
                self.search_input = ui.input(placeholder='搜索股票...').props('outlined dense').classes('w-64 mb-4')
                self.search_input.on('input', self.refresh_table)
                self.table = ui.table(
                    columns=[
                        {"name": "time", "label": "时间", "field": "time", "sortable": True},
                        {"name": "name", "label": "股票", "field": "name", "sortable": True},
                        {"name": "action", "label": "动作", "field": "action", "align": "center"},
                        {"name": "price", "label": "价格", "field": "price", "sortable": True},
                        {"name": "volume", "label": "数量", "field": "volume", "sortable": True},
                        {"name": "amount", "label": "金额", "field": "amount", "sortable": True},
                    ],
                    rows=[],
                    row_key='id'
                ).classes('w-full h-[360px]')
                self.table.add_slot('body-cell-action', '''
                    <q-td :props="props">
                        <q-badge :color="props.value === '买入' ? 'red' : (props.value === '卖出' ? 'blue' : 'grey')">
                            {{ props.value }}
                        </q-badge>
                    </q-td>
                ''')

    async def refresh_all_data(self):
        await self.refresh_chip_stock_list()
        await self.refresh_chip_price()
        await self.refresh_chip_summary()
        await self.refresh_table()

    async def refresh_chip_stock_list(self):
        try:
            if not self.chip_stock_list:
                return
            df = self.db.get_chip_summary("")
            df = df[df["hold_volume"] != 0].drop_duplicates(subset=["name"])
            stock_names = sorted(df["name"].tolist()) if not df.empty else []
            
            self.chip_stock_list.clear()
            with self.chip_stock_list:
                for name in stock_names:
                    ui.item(name, on_click=lambda _=None, n=name: self._on_stock_click(n)).classes(
                        'cursor-pointer hover:bg-blue-50'
                    )
            if stock_names and not self.current_selected_stock:
                await self._on_stock_click(stock_names[0])

        except Exception as e:
            self.log(f"股票列表加载失败: {str(e)}", "error")

    async def _on_stock_click(self, stock_name: str):
        self.current_selected_stock = stock_name
        await self.refresh_chip_price()

    async def refresh_chip_price(self):
        try:
            if not self.current_selected_stock and self.chip_price_table is not None:
                self.chip_price_table.rows = []
                return
            df = self.db.get_chip_price(self.current_selected_stock)
            self.log(f"筹码价格：{self.current_selected_stock}({len(df)}条)", "info")
            
            rows = []
            if not df.empty:
                df = df.astype({"buy_volume": int, "sell_volume": int})
                df["net_volume"] = df["buy_volume"] - df["sell_volume"]
                df["price_key"] = df["name"] + "_" + df["price"].astype(str)
                rows = df.to_dict("records")
            
            if self.chip_price_table is not None:
                self.chip_price_table.rows = rows

        except Exception as e:
            self.log(f"筹码价格加载失败: {str(e)}", "error")

    async def refresh_chip_summary(self):
        try:
            keyword = self.chip_summary_search.value.strip() if (self.chip_summary_search and self.chip_summary_search.value) else ""
            df = self.db.get_chip_summary(keyword)
            self.log(f"筹码统计查询结果：{len(df)} 条数据", "info")
            rows = []
            if not df.empty:
                df = df.astype({"total_buy": int, "total_sell": int, "hold_volume": int})
                df["summary_key"] = df["name"]
                rows = df.to_dict("records")
            if self.chip_summary_table:
                self.chip_summary_table.rows = rows
        except Exception as e:
            self.log(f"筹码统计加载失败: {str(e)}", "error")

    async def refresh_table(self):
        try:
            keyword = self.search_input.value.strip() if (self.search_input and self.search_input.value) else ""
            df = self.db.get_all_trades()
            self.log(f"交易记录查询结果：{len(df)} 条数据", "info")
            rows = []
            if not df.empty:
                if 'time' in df.columns:
                    df['time'] = df['time'].astype(str).str.replace('T', ' ')
                if keyword:
                    df = df[df['name'].str.contains(keyword, na=False)]
                rows = df.to_dict('records')
            if self.table:
                self.table.rows = rows
        except Exception as e:
            self.log(f"交易记录加载失败: {str(e)}", "error")

    async def _parse_trade_image(self, evt: events.UploadEventArguments):
        fname = getattr(evt, 'name', '未知文件')
        self.log(f"处理中: {fname}", "info")
        try:
            img_bytes = await evt.file.read()
            df = self.parser.parse(img_bytes)
            if df.empty:
                self.log(f"无法识别: {fname}", "warn")
                return
            added = self.db.save_trades(df)
            if added > 0:
                self.log(f"完成: {fname} (+{added})", "success")
                await self.refresh_all_data()
            else:
                self.log(f"跳过重复: {fname}", "info")
            if isinstance(evt.sender, ui.upload):
                evt.sender.reset()
        except Exception as e:
            self.log(f"解析崩溃: {str(e)}", "error")

    async def _confirm_clear(self):
        with ui.dialog() as dialog, ui.card().classes('p-6'):
            ui.label('备份并重置数据库？').classes('text-lg font-bold text-red-600')
            ui.label('当前数据将存为 .bak 文件，系统将启用全新数据库。')
            with ui.row().classes('w-full justify-end gap-2 mt-4'):
                ui.button('取消', on_click=dialog.close).props('outline')
                ui.button('确定', on_click=lambda: self._handle_clear(dialog)).props('color=red')
        dialog.open()

    async def _handle_clear(self, dialog):
        if self.db.clear_all_trades():
            ui.notify('旧数据已备份，新数据库已就绪', type='positive')
            self.log("数据库已备份并重置", "success")
            await self.refresh_all_data()
        else:
            ui.notify('重置失败', type='negative')
            self.log("重置失败", "error")
        dialog.close()

    def run(self):
        self.logger.info("Application starting")
        ui.run(title="ChipInSight", port=self.port, host=self.host, reload=True)

if __name__ in {"__main__", "__mp_main__"}:
    ChipInSightUI().run()