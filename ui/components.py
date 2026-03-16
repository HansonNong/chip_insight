from nicegui import ui
from typing import Callable, Any
import plotly.graph_objects as go

class HeaderUI:
    def __init__(self, on_refresh: Callable[..., Any], on_clear: Callable[..., Any]) -> None:
        """Top navigation header with refresh and database reset controls."""
        with ui.header().classes("items-center justify-between bg-slate-800 p-4"):
            with ui.row().classes("items-center"):
                ui.icon("auto_graph", size="lg").classes("text-white")
                ui.label("ChipInSight").classes("text-h5 font-bold text-white")
            with ui.row().classes("items-center gap-3"):
                ui.button("刷新数据", icon="refresh", on_click=on_refresh).props("flat color=white")
                ui.button("备份并重置", icon="history", on_click=on_clear).props("flat color=red-300")

class UploadCardUI:
    def __init__(self, on_multi_upload: Callable[..., Any]) -> None:
        """Card component for uploading trade screenshots."""
        self.uploader: ui.upload | None = None
        self.tip_label: ui.label | None = None
        self._build(on_multi_upload)

    def _build(self, on_multi_upload: Callable[..., Any]) -> None:
        with ui.card().classes("p-2 sm:p-6"):
            ui.label("同步记录").classes("text-lg font-bold mb-2")
            with ui.row().classes("items-start gap-6"):
                self.uploader = ui.upload(
                    label="上传股票交易截图 (支持批量)",
                    on_multi_upload=on_multi_upload,
                    multiple=True,
                    auto_upload=True
                ).classes("flex-grow h-32")
                with ui.column().classes("w-64 p-4 bg-gray-50 rounded"):
                    ui.label("系统状态").classes("text-xs font-bold text-gray-400 uppercase")
                    self.tip_label = ui.label("就绪").classes("text-sm text-gray-600 mt-1")

    def reset(self) -> None:
        """Reset the uploader state."""
        if self.uploader:
            self.uploader.reset()

class StockSelectorUI:
    def __init__(self, on_change: Callable[..., Any]) -> None:
        """Unified stock selector for the application."""
        self.select: ui.select | None = None
        self.on_change = on_change
        self._build()

    def _build(self) -> None:
        with ui.card().classes("p-4 sm:p-6 shadow-sm w-full mb-2 flex flex-row items-center gap-4"):
            ui.icon("troubleshoot", size="sm").classes("text-blue-500")
            ui.label("全局分析目标股票:").classes("text-lg font-bold")
            self.select = ui.select(
                options=[],
                with_input=True,
                on_change=self.on_change,
                label="搜索或选择股票..."
            ).classes("w-64")

    def set_options(self, options: list[str]) -> None:
        if self.select:
            self.select.options = options
            self.select.update()

    def set_value(self, value: str) -> None:
        if self.select:
            self.select.value = value

class SellMatchUI:
    def __init__(self, on_row_click: Callable[..., Any]) -> None:
        """UI for matching sell orders with historical buy orders to calculate profit."""
        self.sell_match_table: ui.table | None = None
        self.on_row_click = on_row_click
        self._build()

    def _build(self) -> None:
        with ui.card().classes("p-2 sm:p-6 w-full mb-4"):
            ui.label("卖出筹码匹配").classes("text-xl font-bold mb-4")
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
            ).classes("h-[320px] w-full")

            # Vue slots for custom cell rendering
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

    def set_rows(self, rows: list[dict[str, Any]]) -> None:
        if self.sell_match_table:
            self.sell_match_table.rows = rows

class ChipPriceUI:
    def __init__(self, on_gen_plot: Callable[..., Any]) -> None:
        """UI for visualizing market chip distribution and holding details."""
        self.chip_price_table: ui.table | None = None
        self.chip_dist_plot: ui.plotly | None = None 
        self.on_gen_plot = on_gen_plot
        self._build()
    
    def _build(self) -> None:
        with ui.card().classes("p-4 sm:p-6 shadow-sm mb-6 w-full"):
            ui.label("筹码价格 + 分布").classes("text-xl font-bold mb-4")
            with ui.column().classes("gap-6 w-full"):
                with ui.column().classes("w-full"):
                    ui.label("价格分布明细").classes("text-sm font-semibold mb-2")
                    self.chip_price_table = ui.table(
                        columns=[
                            {"name": "name", "label": "股票", "field": "name", "sortable": True},
                            {"name": "price", "label": "价格", "field": "price", "sortable": True},
                            {"name": "net_volume", "label": "数量", "field": "net_volume", "sortable": True},
                        ],
                        rows=[],
                        row_key="price_key"
                    ).classes("h-[260px] w-full")

                    self.chip_price_table.add_slot("body-cell-net_volume", '''
                        <q-td :props="props">
                            <q-badge :color="props.value > 0 ? 'red' : (props.value < 0 ? 'blue' : 'grey')">
                                {{ props.value }}
                            </q-badge>
                        </q-td>
                    ''')
                
                with ui.column().classes("w-full"):
                    with ui.row().classes("items-center gap-4 mb-2"):
                        ui.label("筹码分布对比").classes("text-sm font-semibold")
                        ui.button("生成分布图", icon="insights", on_click=self.on_gen_plot).props("outline dense color=primary")
                    # Placeholder figure to avoid empty init errors
                    self.chip_dist_plot = ui.plotly(figure=go.Figure()).classes("h-[500px] w-full")
    
    def set_rows(self, rows: list[dict[str, Any]]) -> None:
        if self.chip_price_table:
            self.chip_price_table.rows = rows
    
    def set_chip_plot(self, figure: go.Figure) -> None: 
        if self.chip_dist_plot:
            self.chip_dist_plot.figure = figure
            self.chip_dist_plot.update()

class SummaryUI:
    def __init__(self, on_search: Callable[..., Any], on_code_edit: Callable[..., Any], on_auto_fill: Callable[..., Any]) -> None:
        """Dashboard summary of all stock holdings and their configurations."""
        self.chip_summary_search: ui.input | None = None
        self.chip_summary_table: ui.table | None = None
        self.on_search = on_search
        self.on_code_edit = on_code_edit
        self.on_auto_fill = on_auto_fill
        self._build()

    def _build(self) -> None:
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
                    {"name": "code", "label": "股票代码", "field": "code", "sortable": True, "align": "center"},
                    {"name": "name", "label": "股票", "field": "name", "sortable": True},
                    {"name": "float_shares", "label": "自由流通(亿)", "field": "float_shares", "sortable": True, "align": "center"}, 
                    {"name": "total_buy", "label": "总买入", "field": "total_buy", "sortable": True},
                    {"name": "total_sell", "label": "总卖出", "field": "total_sell", "sortable": True},
                    {"name": "hold_volume", "label": "当前持仓", "field": "hold_volume", "sortable": True},
                    {"name": "net_profit", "label": "已配对净盈亏", "field": "net_profit", "sortable": True},
                ],
                rows=[],
                row_key="summary_key"
            ).classes("h-[260px]")

            self.chip_summary_table.add_slot("body-cell-code", '''
                <q-td :props="props" @click.stop="$parent.$emit('edit_code', props.row)" class="cursor-pointer text-blue-500 font-medium">
                    {{ props.value || '点此设置' }}
                    <q-icon name="edit" size="xs" class="ml-1 text-grey-4" />
                </q-td>
            ''')

            self.chip_summary_table.add_slot("body-cell-float_shares", '''
                <q-td :props="props" @click.stop="$parent.$emit('edit_float', props.row)" class="cursor-pointer text-blue-500 font-medium">
                    {{ props.value > 0 ? props.value : '点此设置' }}
                    <q-icon name="edit" size="xs" class="ml-1 text-grey-4" />
                </q-td>
            ''')

            self.chip_summary_table.add_slot("body-cell-hold_volume", '''
                <q-td :props="props">
                    <q-badge :color="props.value > 0 ? 'red' : (props.value < 0 ? 'blue' : 'grey')">
                        {{ props.value }}
                    </q-badge>
                </q-td>
            ''')
            
            self.chip_summary_table.add_slot("body-cell-net_profit", '''
                <q-td :props="props">
                    <q-badge :color="props.value > 0 ? 'red' : (props.value < 0 ? 'blue' : 'grey')">
                        {{ (props.value || 0).toFixed(2) }}
                    </q-badge>
                </q-td>
            ''')

    def get_search_keyword(self) -> str:
        return self.chip_summary_search.value.strip() if (self.chip_summary_search and self.chip_summary_search.value) else ""

    def set_rows(self, rows: list[dict[str, Any]]) -> None:
        if self.chip_summary_table:
            self.chip_summary_table.rows = rows

class TradeTableUI:
    def __init__(self, on_search: Callable[..., Any]) -> None:
        """Detailed transaction ledger with search capability."""
        self.search_input: ui.input | None = None
        self.table: ui.table | None = None
        self.on_search = on_search
        self._build()

    def _build(self) -> None:
        with ui.card().classes("p-2 sm:p-6"):
            ui.label("流水明细").classes("text-xl font-bold mb-4")
            self.search_input = ui.input(
                placeholder="搜索股票...", 
                on_change=self.on_search
            ).props("outlined dense").classes("w-64 mb-4")

            self.table = ui.table(
                columns=[
                    {"name": "date", "label": "日期", "field": "date", "sortable": True},
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

            text_slot_template = '''
                <q-td :props="props" class="cursor-pointer">
                    {{{{ props.value }}}}
                    <q-icon name="edit" size="xs" class="ml-1 text-grey-5" />
                    <q-popup-edit v-model="props.row.{field}" v-slot="scope" 
                                  @save="(val) => $parent.$emit('update_trade', {{'id': props.row.id, 'field': '{field}', 'value': val}})">
                        <q-input v-model="scope.value" dense autofocus @keyup.enter="scope.set" {input_props} />
                    </q-popup-edit>
                </q-td>
            '''
            
            number_slot_template = '''
                <q-td :props="props" class="cursor-pointer">
                    {{{{ props.value }}}}
                    <q-icon name="edit" size="xs" class="ml-1 text-grey-5" />
                    <q-popup-edit v-model.number="props.row.{field}" v-slot="scope" 
                                  @save="(val) => $parent.$emit('update_trade', {{'id': props.row.id, 'field': '{field}', 'value': val}})">
                        <q-input type="number" v-model.number="scope.value" dense autofocus @keyup.enter="scope.set" />
                    </q-popup-edit>
                </q-td>
            '''

            self.table.add_slot('body-cell-date', text_slot_template.format(field='date', input_props='mask="####-##-##"'))
            self.table.add_slot('body-cell-time', text_slot_template.format(field='time', input_props='mask="##:##:##"'))
            self.table.add_slot('body-cell-name', text_slot_template.format(field='name', input_props=''))
            self.table.add_slot('body-cell-price', number_slot_template.format(field='price'))
            self.table.add_slot('body-cell-volume', number_slot_template.format(field='volume'))


    def get_search_keyword(self) -> str:
        return self.search_input.value.strip() if (self.search_input and self.search_input.value) else ""

    def set_rows(self, rows: list[dict[str, Any]]) -> None:
        if self.table:
            self.table.rows = rows

class BuyMatchDialogUI:
    def __init__(self, on_match: Callable[..., Any]) -> None:
        """Dialog for selecting specific buy records to offset a sell record."""
        self.dialog: ui.dialog | None = None
        self.available_buy_table: ui.table | None = None
        self.on_match = on_match
        self._build()

    def _build(self) -> None:
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

    def set_rows(self, rows: list[dict[str, Any]]) -> None:
        if self.available_buy_table:
            self.available_buy_table.rows = rows

    def open(self) -> None:
        if self.dialog:
            self.dialog.open()
            
    def close(self) -> None:
        if self.dialog:
            self.dialog.close()