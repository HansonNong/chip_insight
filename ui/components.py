from nicegui import ui
from typing import Callable
import plotly.graph_objects as go


class HeaderUI:
    def __init__(self, on_refresh: Callable, on_clear: Callable):
        with ui.header().classes("items-center justify-between bg-slate-800 p-4"):
            with ui.row().classes("items-center"):
                ui.icon("auto_graph", size="lg").classes("text-white")
                ui.label("ChipInSight").classes("text-h5 font-bold text-white")
            with ui.row().classes("items-center gap-3"):
                ui.button("刷新数据", icon="refresh", on_click=on_refresh).props("flat color=white")
                ui.button("备份并重置", icon="history", on_click=on_clear).props("flat color=red-300")

class UploadCardUI:
    def __init__(self, on_upload: Callable):
        self.uploader: ui.upload | None = None
        self.tip_label: ui.label | None = None
        self._build(on_upload)
    def _build(self, on_upload):
        with ui.card().classes("p-2 sm:p-6"):
            ui.label("同步记录").classes("text-lg font-bold mb-2")
            with ui.row().classes("items-start gap-6"):
                self.uploader = ui.upload(
                    label="上传股票交易截图 (支持批量)",
                    on_upload=on_upload,
                    multiple=True,
                    auto_upload=True
                ).classes("flex-grow h-32")
                with ui.column().classes("w-64 p-4 bg-gray-50 rounded"):
                    ui.label("系统状态").classes("text-xs font-bold text-gray-400 uppercase")
                    self.tip_label = ui.label("就绪").classes("text-sm text-gray-600 mt-1")
    def reset(self):
        if self.uploader:
            self.uploader.reset()

class SellMatchUI:
    def __init__(self, on_stock_switch: Callable, on_row_click: Callable):
        self.match_stock_list: ui.list | None = None
        self.sell_match_table: ui.table | None = None
        self.on_stock_switch = on_stock_switch
        self.on_row_click = on_row_click
        self._build()
    def _build(self):
        with ui.card().classes("p-2 sm:p-6"):
            ui.label("卖出筹码匹配").classes("text-xl font-bold mb-4")
            with ui.row().classes("gap-4"):
                with ui.column().classes("w-56 h-[320px] overflow-y-auto border rounded p-2"):
                    ui.label("选择股票").classes("text-sm font-semibold mb-2")
                    self.match_stock_list = ui.list().classes("w-full")
                with ui.column().classes("flex-1"):
                    self.sell_match_table = ui.table(
                        columns=[
                            {"name": "time", "label": "卖出时间", "field": "time", "sortable": True},
                            {"name": "price", "label": "卖出价", "field": "price", "sortable": True},
                            {"name": "volume", "label": "卖出量", "field": "volume", "sortable": True},
                            {"name": "profit", "label": "盈利", "field": "profit", "sortable": True},
                            {"name": "profit_pct", "label": "盈利率", "field": "profit_pct", "sortable": True},
                            {"name": "annual", "label": "年化(单利)", "field": "annual", "sortable": True},
                            {"name": "status", "label": "状态", "field": "status", "align": "center"},
                        ],
                        rows=[],
                        row_key="sell_id"
                    ).classes("h-[320px]")
                    self.sell_match_table.add_slot("body-cell-profit", '''
                        <q-td :props="props">
                            <div v-if="props.row.status === '未匹配'" class="text-gray-600">
                                {{ props.value.toFixed(2) }}
                            </div>
                            <q-badge v-else :color="props.value >= 0 ? 'red' : 'blue'">
                                {{ props.value.toFixed(2) }}
                            </q-badge>
                        </q-td>
                    ''')
                    self.sell_match_table.add_slot("body-cell-profit_pct", '''
                        <q-td :props="props">
                            <div v-if="props.row.status === '未匹配'" class="text-gray-600">
                                {{ (props.value*100).toFixed(2) }}%
                            </div>
                            <q-badge v-else :color="props.value >= 0 ? 'red' : 'blue'">
                                {{ (props.value*100).toFixed(2) }}%
                            </q-badge>
                        </q-td>
                    ''')
                    self.sell_match_table.add_slot("body-cell-annual", '''
                        <q-td :props="props">
                            <div v-if="props.row.status === '未匹配'" class="text-gray-600">
                                {{ (props.value*100).toFixed(2) }}%
                            </div>
                            <q-badge v-else :color="props.value >= 0 ? 'red' : 'blue'">
                                {{ (props.value*100).toFixed(2) }}%
                            </q-badge>
                        </q-td>
                    ''')
                    self.sell_match_table.on("rowClick", self.on_row_click)
    def set_rows(self, rows):
        if self.sell_match_table:
            self.sell_match_table.rows = rows
    def clear_stock_list(self):
        if self.match_stock_list:
            self.match_stock_list.clear()
    def add_stock_item(self, name, callback):
        if self.match_stock_list:
            with self.match_stock_list:
                ui.item(name, on_click=callback).classes("cursor-pointer hover:bg-blue-50")

class ChipPriceUI:
    def __init__(self, on_stock_click: Callable):
        self.chip_stock_list: ui.list | None = None
        self.chip_price_table: ui.table | None = None
        self.chip_dist_plot: ui.plotly | None = None 
        self.on_stock_click = on_stock_click
        self._build()
    
    def _build(self):
        with ui.card().classes("p-4 sm:p-6 shadow-sm mb-6"):
            ui.label("筹码价格 + 分布").classes("text-xl font-bold mb-4")
            
            with ui.column().classes("gap-6"):
                
                with ui.row().classes("gap-4 flex-col md:flex-row items-stretch"):
                    
                    with ui.column().classes("md:w-56"):
                        ui.label("持仓股票").classes("text-sm font-semibold mb-2")
                        self.chip_stock_list = ui.list().classes("h-[200px] md:h-[260px] overflow-y-auto border rounded p-2")
                    
                    with ui.column().classes("flex-grow"):
                        ui.label("价格分布明细").classes("text-sm font-semibold mb-2")
                        self.chip_price_table = ui.table(
                            columns=[
                                {"name": "name", "label": "股票", "field": "name", "sortable": True},
                                {"name": "price", "label": "价格", "field": "price", "sortable": True},
                                {"name": "net_volume", "label": "数量", "field": "net_volume", "sortable": True},
                            ],
                            rows=[],
                            row_key="price_key"
                        ).classes("h-[260px]")

                    self.chip_price_table.add_slot("body-cell-net_volume", '''
                        <q-td :props="props">
                            <q-badge :color="props.value > 0 ? 'red' : (props.value < 0 ? 'blue' : 'grey')">
                                {{ props.value }}
                            </q-badge>
                        </q-td>
                    ''')
                
                with ui.column().classes("flex-1"):
                    ui.label("筹码分布对比").classes("text-sm font-semibold mb-2")
                    self.chip_dist_plot = ui.plotly(figure=go.Figure()).classes("h-[500px] w-[500px]")
    
    def set_rows(self, rows):
        if self.chip_price_table:
            self.chip_price_table.rows = rows
    
    def set_chip_plot(self, figure: go.Figure): 
        if self.chip_dist_plot:
            self.chip_dist_plot.figure = figure
            self.chip_dist_plot.update()
    
    def clear_stock_list(self):
        if self.chip_stock_list:
            self.chip_stock_list.clear()
    
    def add_stock_item(self, name, callback):
        if self.chip_stock_list:
            with self.chip_stock_list:
                ui.item(name, on_click=callback).classes("cursor-pointer hover:bg-blue-50")

class SummaryUI:
    def __init__(self, on_search: Callable, on_code_edit: Callable, on_auto_fill: Callable):
        self.chip_summary_search: ui.input | None = None
        self.chip_summary_table: ui.table | None = None
        self.on_search = on_search
        self.on_code_edit = on_code_edit
        self.on_auto_fill = on_auto_fill
        self._build()

    def _build(self):
        with ui.card().classes("p-2 sm:p-6"):
            ui.label("筹码统计").classes("text-xl font-bold mb-4")
            ui.button(
                "自动补全代码", 
                icon="magic_button", 
                on_click=self.on_auto_fill
            ).props("flat dense color=primary")


            self.chip_summary_search = ui.input(
                placeholder="搜索股票...", 
                on_change=self.on_search
            ).props("outlined dense").classes("w-64 mb-4")
                        
            self.chip_summary_table = ui.table(
                columns=[
                    {"name": "code", "label": "股票代码", "field": "code", "sortable": True},
                    {"name": "name", "label": "股票", "field": "name", "sortable": True},
                    {"name": "total_buy", "label": "总买入", "field": "total_buy", "sortable": True},
                    {"name": "total_sell", "label": "总卖出", "field": "total_sell", "sortable": True},
                    {"name": "hold_volume", "label": "当前持仓", "field": "hold_volume", "sortable": True},
                ],
                rows=[],
                row_key="summary_key"
            ).classes("h-[260px]")

            self.chip_summary_table.add_slot("body-cell-code", '''
                <q-td :props="props">
                    <div class="cursor-pointer text-blue-600 hover:underline px-1"
                        @click="$emit('rowClick', props)">
                        {{ props.value || '点击填写' }}
                    </div>
                </q-td>
            ''')

            self.chip_summary_table.add_slot("body-cell-hold_volume", '''
                <q-td :props="props">
                    <q-badge :color="props.value > 0 ? 'red' : (props.value < 0 ? 'blue' : 'grey')">
                        {{ props.value }}
                    </q-badge>
                </q-td>
            ''')

            self.chip_summary_table.on('rowClick', self._handle_click)

    async def _handle_click(self, e):
        row = e.args[1]
        await self.on_code_edit(row)

    def get_search_keyword(self):
        return self.chip_summary_search.value.strip() if (self.chip_summary_search and self.chip_summary_search.value) else ""

    def set_rows(self, rows):
        if self.chip_summary_table:
            self.chip_summary_table.rows = rows

class TradeTableUI:
    def __init__(self, on_search: Callable):
        self.search_input: ui.input | None = None
        self.table: ui.table | None = None
        self.on_search = on_search
        self._build()
    def _build(self):
        with ui.card().classes("p-2 sm:p-6"):
            ui.label("流水明细").classes("text-xl font-bold mb-4")
            
            self.search_input = ui.input(
                placeholder="搜索股票...", 
                on_change=self.on_search
            ).props("outlined dense").classes("w-64 mb-4")

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
                row_key="id"
            ).classes("h-[360px]")
            self.table.add_slot("body-cell-action", '''
                <q-td :props="props">
                    <q-badge :color="props.value === '买入' ? 'red' : (props.value === '卖出' ? 'blue' : 'grey')">
                        {{ props.value }}
                    </q-badge>
                </q-td>
            ''')
    def get_search_keyword(self):
        return self.search_input.value.strip() if (self.search_input and self.search_input.value) else ""
    def set_rows(self, rows):
        if self.table:
            self.table.rows = rows

class BuyMatchDialogUI:
    def __init__(self, on_match: Callable):
        self.dialog: ui.dialog | None = None
        self.available_buy_table: ui.table | None = None
        self.on_match = on_match
        self._build()

    def _build(self):
        self.dialog = ui.dialog()
        with self.dialog, ui.card().classes("w-[750px] p-4"):
            ui.label("选择或撤销匹配的买入筹码").classes("text-lg font-bold mb-2")
            self.available_buy_table = ui.table(
                columns=[
                    {"name": "time", "label": "买入时间", "field": "time", "sortable": True},
                    {"name": "price", "label": "买入价", "field": "price", "sortable": True},
                    {"name": "remain", "label": "可用/总额", "field": "display_vol", "sortable": True},
                    {"name": "status", "label": "当前状态", "field": "status", "align": "center"},
                    {"name": "act", "label": "操作", "field": "act", "align": "center"},
                ],
                rows=[],
                row_key="buy_id"
            ).classes("h-[400px]")
            
            self.available_buy_table.add_slot("body-cell-status", '''
                <q-td :props="props">
                    <q-badge :color="props.value === '已选择' ? 'green' : 'grey'">
                        {{ props.value }}
                    </q-badge>
                </q-td>
            ''')

            self.available_buy_table.add_slot("body-cell-act", '''
                <q-td :props="props">
                    <q-btn size="sm" 
                        :color="props.row.is_matched ? 'orange' : 'primary'" 
                        :label="props.row.is_matched ? '撤销匹配' : '进行匹配'"
                        @click="() => $parent.$emit('match', props.row)" />
                </q-td>
            ''')
            self.available_buy_table.on("match", self.on_match)
            
            ui.button("关闭", on_click=self.dialog.close).classes("mt-3 w-full").props("outline")

    def set_rows(self, rows):
        if self.available_buy_table:
            self.available_buy_table.rows = rows

    def open(self):
        if self.dialog:
            self.dialog.open()
            
    def close(self):
        if self.dialog:
            self.dialog.close()