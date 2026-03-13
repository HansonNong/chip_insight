from nicegui import ui, events
import plotly.graph_objects as go
import asyncio 

from .config import Config
from .logger import AppLogger
from .service import TradeService
from .components import (
    HeaderUI, UploadCardUI, SellMatchUI, ChipPriceUI,
    SummaryUI, TradeTableUI, BuyMatchDialogUI
)
from db.database import TradeDatabase
from core.parse_input import TradeImageParser
from core.fetch_data import get_stock_data
from core.visualize_cost import ChipDistVisualizer


class ChipInSightApp:
    def __init__(self):
        self.logger = AppLogger()
        self.db = TradeDatabase()
        self.parser = TradeImageParser()
        self.service = TradeService(self.db, self.parser)
        self.current_selected_stock: str = ""
        self.current_matching_sell_id: str = ""
        self.chip_visualizer = ChipDistVisualizer(  
            bin_count=300, decay_threshold=1e-3, smoothing=1.5, contrast=3
        )

        self.header: HeaderUI | None = None
        self.upload_card: UploadCardUI | None = None
        self.sell_match_ui: SellMatchUI | None = None
        self.chip_price_ui: ChipPriceUI | None = None
        self.summary_ui: SummaryUI | None = None
        self.trade_table_ui: TradeTableUI | None = None
        self.buy_dialog: BuyMatchDialogUI | None = None

        self._build_ui()
        ui.timer(0.5, self.refresh_all_data, once=True)

    def _build_ui(self):
        ui.page_title(Config.APP_TITLE)
        self.header = HeaderUI(on_refresh=self.refresh_all_data, on_clear=self._confirm_clear)
        ui.add_head_html('<meta name="viewport" content="width=device-width, initial-scale=0.6, maximum-scale=1.0">')
        with ui.column().classes("w-full mx-auto my-1 px-1 sm:px-2 min-h-[80vh] sm:max-w-6xl"):
            self.upload_card = UploadCardUI(on_upload=self._parse_trade_image)

            if self.upload_card and self.upload_card.tip_label:
                self.logger.set_tip_label(self.upload_card.tip_label)

            self.sell_match_ui = SellMatchUI(
                on_stock_switch=self._on_stock_switch,
                on_row_click=self._open_match_dialog
            )
            self.chip_price_ui = ChipPriceUI(on_stock_click=self._on_stock_click)
            self.summary_ui = SummaryUI(on_search=self.refresh_chip_summary, on_code_edit=self._edit_stock_code)
            self.trade_table_ui = TradeTableUI(on_search=self.refresh_table)

        self.buy_dialog = BuyMatchDialogUI(on_match=self._do_match)

    async def refresh_match_stock_list(self):
        df = self.service.get_chip_summary("")
        df = df[df["hold_volume"] != 0].drop_duplicates("name")
        names = sorted(df["name"].tolist()) if not df.empty else []

        if self.sell_match_ui:
            self.sell_match_ui.clear_stock_list()
            for name in names:
                self.sell_match_ui.add_stock_item(name, lambda _, n=name: self._on_stock_switch(n))

        if names and not self.current_selected_stock:
            await self._on_stock_switch(names[0])

    async def _on_stock_switch(self, stock_name: str):
        self.current_selected_stock = stock_name
        await self.refresh_sell_match_table()
        await self.refresh_chip_price()

    async def refresh_sell_match_table(self):
        if not self.current_selected_stock or not self.sell_match_ui:
            return

        df = self.service.get_sell_records_with_match(self.current_selected_stock)
        rows = []
        if not df.empty:
            df["time"] = df["time"].astype(str).str.replace("T", " ")
            for _, r in df.iterrows():
                profit = round(r.get("profit", 0), 2)
                profit_pct = round(r.get("profit_pct", 0), 4)
                buy_time = r.get("buy_time") or r.get("buy_date") or r["time"]
                annual = self.service.calc_annual(buy_time, r["time"], profit_pct)
                rows.append({
                    "sell_id": str(r["id"]),
                    "time": r["time"],
                    "price": round(r["sell_price"], 2),
                    "volume": int(r["sell_volume"]),
                    "profit": profit,
                    "profit_pct": profit_pct,
                    "annual": annual,
                    "status": r.get("match_status", "未匹配"),
                })
        self.sell_match_ui.set_rows(rows)

    async def _open_match_dialog(self, e):
        row = e.args[1]
        self.current_matching_sell_id = str(row["sell_id"])
        await self._load_available_buys()
        if self.buy_dialog:
            self.buy_dialog.open()

    async def _load_available_buys(self):
        if not self.buy_dialog:
            return
        
        df = self.service.get_available_buys(self.current_selected_stock)
        rows = []
        if not df.empty:
            df["time"] = df["time"].astype(str).str.replace("T", " ")
            for _, r in df.iterrows():
                rows.append({
                    "buy_id": str(r["id"]),
                    "time": r["time"],
                    "price": round(r["price"], 2),
                    "remain": int(r["remain_volume"]),
                    "act": "选择"
                })
        self.buy_dialog.set_rows(rows)

    async def _do_match(self, e):
        buy_row = e.args[1]
        buy_id = buy_row["buy_id"]
        self.service.remove_match(self.current_matching_sell_id)
        
        ok = self.service.create_match(self.current_matching_sell_id, buy_id)
        if ok:
            self.logger.success("重新匹配成功！")
            await self.refresh_sell_match_table()
            await self.refresh_chip_price()
            if self.buy_dialog:
                self.buy_dialog.close()

    async def refresh_chip_stock_list(self):
        df = self.service.get_chip_summary("")
        df = df[df["hold_volume"] != 0].drop_duplicates("name")
        names = sorted(df["name"].tolist()) if not df.empty else []

        if self.chip_price_ui:
            self.chip_price_ui.clear_stock_list()
            for name in names:
                self.chip_price_ui.add_stock_item(name, lambda _, n=name: self._on_stock_click(n))

        if names and not self.current_selected_stock:
            await self._on_stock_click(names[0])

    async def _on_stock_click(self, stock_name: str):
        self.current_selected_stock = stock_name
        await self.refresh_chip_price()
        await self.refresh_chip_dist_plot() 

    async def get_own_chips(self, stock_name: str) -> list[tuple[float, int]]:
        df = self.service.get_chip_price(stock_name)
        if df.empty:
            return []
        
        df = df.astype({"buy_volume": int, "sell_volume": int})
        df["net_volume"] = df["buy_volume"] - df["sell_volume"]
        df = df[df["net_volume"] > 0]

        return list(zip(df["price"].round(2), df["net_volume"]))
    
    async def refresh_chip_dist_plot(self) -> None:
        if not self.current_selected_stock or not self.chip_price_ui:
            return

        # 1. Get stock code
        summary_df = self.service.get_chip_summary(self.current_selected_stock)
        if summary_df.empty:
            self.chip_price_ui.set_chip_plot(go.Figure())
            self.logger.warn(f"{self.current_selected_stock} 无代码信息，无法获取行情")
            ui.notify(f"{self.current_selected_stock} 无代码信息，无法获取行情", position="left")
            return
        
        stock_code = summary_df.iloc[0].get("code", "")
        if not stock_code:
            self.chip_price_ui.set_chip_plot(go.Figure())
            self.logger.warn(f"{self.current_selected_stock} 未配置股票代码，请先在筹码统计中填写")
            ui.notify(f"{self.current_selected_stock} 未配置股票代码，请先在筹码统计中填写", position="left")
            return

        # 2. Get stock data
        self.logger.info(f"正在获取 {self.current_selected_stock}({stock_code}) 行情数据...")
        ui.notify(f"正在获取 {self.current_selected_stock} 行情数据...", position="left")
        await asyncio.sleep(0.01)

        kline_df, std_code = get_stock_data(stock_code)
        if kline_df is None or kline_df.empty:
            self.chip_price_ui.set_chip_plot(go.Figure())
            self.logger.warn(f"{self.current_selected_stock} 行情数据获取失败")
            ui.notify(f"{self.current_selected_stock} 行情数据获取失败", position="left")
            return

        # 3. Generate chip distribution data
        dist_data = self.chip_visualizer.generate_distribution(kline_df)
        if not dist_data:
            self.chip_price_ui.set_chip_plot(go.Figure())
            return

        # 4. Get own chips for annotation on the plot
        own_chips = await self.get_own_chips(self.current_selected_stock)

        # 5. Render plot with distribution data and own chips
        fig = self.chip_visualizer.render_plot(
            data=dist_data,
            own_chips=own_chips
        )

        # 6. Update the UI with the new plot
        self.chip_price_ui.set_chip_plot(fig)
        if self.chip_price_ui.chip_dist_plot:
            self.chip_price_ui.chip_dist_plot.update()
        self.logger.success(f"{self.current_selected_stock} 筹码分布图生成完成")
        ui.notify(f"{self.current_selected_stock} 筹码分布图生成完成", position="left")

    async def refresh_chip_price(self):
        if not self.current_selected_stock or not self.chip_price_ui:
            return

        df = self.service.get_chip_price(self.current_selected_stock)
        rows = []
        if not df.empty:
            df = df.astype({"buy_volume": int, "sell_volume": int})
            df["net_volume"] = df["buy_volume"] - df["sell_volume"]
            df["price_key"] = df["name"] + "_" + df["price"].astype(str)
            rows = df.to_dict("records")
        self.chip_price_ui.set_rows(rows)

    async def refresh_chip_summary(self):
        if not self.summary_ui:
            return
        keyword = self.summary_ui.get_search_keyword()
        df = self.service.get_chip_summary(keyword)
        rows = []
        if not df.empty:
            df = df.astype({"total_buy": int, "total_sell": int, "hold_volume": int})
            df["summary_key"] = df["name"]
            if "code" not in df.columns:
                df["code"] = ""
            rows = df.to_dict("records")
        self.summary_ui.set_rows(rows)

    async def _edit_stock_code(self, row):
        stock_name = row["name"]
        current_code = row.get("code", "")

        with ui.dialog() as dialog, ui.card().classes("p-6 w-[400px]"):
            ui.label(f"编辑股票代码：{stock_name}").classes("text-lg font-bold mb-4")
            code_input = ui.input(
                label="股票代码",
                value=current_code,
                placeholder="请输入6位数字股票代码"
            ).props("outlined dense").classes("w-full mb-4")

            async def _save():
                new_code = code_input.value.strip()

                if not new_code.isdigit() or len(new_code) != 6:
                    ui.notify("股票代码必须是6位数字", type="negative", position="left")
                    return

                self.service.update_stock_code(stock_name, new_code)
                ui.notify(f"{stock_name} 代码保存成功：{new_code}", type="positive", position="left")
                await self.refresh_chip_summary()
                dialog.close()

            with ui.row().classes("justify-end gap-3 mt-4"):
                ui.button("取消", on_click=dialog.close).props("flat")
                ui.button("保存", on_click=_save, color="blue-5")

        dialog.open()

    async def refresh_table(self) -> None:
        if not self.trade_table_ui:
            return
        
        keyword = self.trade_table_ui.get_search_keyword()
        df = self.service.get_all_trades()
        if keyword:
            df = df[df["name"].str.contains(keyword, na=False)]
        if "time" in df.columns:
            df["time"] = df["time"].astype(str).str.replace("T", " ")
        if "code" not in df.columns:
            df["code"] = ""

        rows = df.to_dict("records") if not df.empty else []
        self.trade_table_ui.set_rows(rows)

    async def _parse_trade_image(self, evt: events.UploadEventArguments):
        fname = evt.file.name
        self.logger.info(f"处理中：{fname}")
        img_bytes = await evt.file.read()
        df = self.service.parse_image(img_bytes)
        if df.empty:
            self.logger.warn(f"无法识别：{fname}")
            return
        added = self.service.save_trades(df)
        if added > 0:
            self.logger.success(f"完成：{fname}(+{added})")
            await self.refresh_all_data()
        else:
            self.logger.info(f"跳过重复：{fname}")
        if self.upload_card:
            self.upload_card.reset()

    async def _confirm_clear(self):
        with ui.dialog() as dialog, ui.card().classes("p-6"):
            ui.label("备份并重置数据库？").classes("text-lg font-bold text-red-600")
            ui.label("当前数据将存为 .bak 文件，系统启用新库")
            with ui.row().classes("justify-end gap-2 mt-4"):
                ui.button("取消", on_click=dialog.close).props("outline")
                ui.button("确定", on_click=lambda: self._handle_clear(dialog), color="red")
        dialog.open()

    async def _handle_clear(self, dialog):
        if self.service.backup_and_clear():
            ui.notify("备份并重置成功", type="positive", position="left")
            await self.refresh_all_data()
        else:
            ui.notify("重置失败", type="negative", position="left")
        dialog.close()

    async def refresh_all_data(self):
        await self.refresh_match_stock_list()
        await self.refresh_sell_match_table()
        await self.refresh_chip_stock_list()
        await self.refresh_chip_price()
        await self.refresh_chip_summary()
        await self.refresh_table()

    def run(self):
        ui.run(
            title=Config.APP_TITLE, 
            host=Config.HOST, 
            port=Config.PORT, 
            reload=Config.RELOAD, 
            show=Config.SHOW
        )