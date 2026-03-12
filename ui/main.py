from nicegui import ui, events

from .config import Config
from .logger import AppLogger
from .service import TradeService
from .components import (
    HeaderUI, UploadCardUI, SellMatchUI, ChipPriceUI,
    SummaryUI, TradeTableUI, BuyMatchDialogUI
)
from db.database import TradeDatabase
from core.parse_input import TradeImageParser

class ChipInSightApp:
    def __init__(self):
        self.logger = AppLogger()
        self.db = TradeDatabase()
        self.parser = TradeImageParser()
        self.service = TradeService(self.db, self.parser)

        self.current_selected_stock = ""
        self.current_matching_sell_id = ""

        self.header = None
        self.upload_card = None
        self.sell_match_ui = None
        self.chip_price_ui = None
        self.summary_ui = None
        self.trade_table_ui = None
        self.buy_dialog = None

        self._build_ui()
        ui.timer(0.5, self.refresh_all_data, once=True)

    def _build_ui(self):
        ui.page_title(Config.APP_TITLE)
        self.header = HeaderUI(on_refresh=self.refresh_all_data, on_clear=self._confirm_clear)
        with ui.column().classes("w-full max-w-6xl mx-auto my-6 p-4 min-h-[80vh]"):
            self.upload_card = UploadCardUI(on_upload=self._parse_trade_image)
            self.logger.set_tip_label(self.upload_card.tip_label)

            self.sell_match_ui = SellMatchUI(
                on_stock_switch=self._on_stock_switch,
                on_row_click=self._open_match_dialog
            )
            self.chip_price_ui = ChipPriceUI(on_stock_click=self._on_stock_click)
            self.summary_ui = SummaryUI(on_search=self.refresh_chip_summary)
            self.trade_table_ui = TradeTableUI(on_search=self.refresh_table)

        self.buy_dialog = BuyMatchDialogUI(on_match=self._do_match)

    async def refresh_match_stock_list(self):
        df = self.service.get_chip_summary("")
        df = df[df["hold_volume"] != 0].drop_duplicates("name")
        names = sorted(df["name"].tolist()) if not df.empty else []
        self.sell_match_ui.clear_stock_list()
        for name in names:
            self.sell_match_ui.add_stock_item(name, lambda _, n=name: self._on_stock_switch(n))
        if names and not self.current_selected_stock:
            await self._on_stock_switch(names[0])

    async def _on_stock_switch(self, stock_name):
        self.current_selected_stock = stock_name
        await self.refresh_sell_match_table()
        await self.refresh_chip_price()

    async def refresh_sell_match_table(self):
        if not self.current_selected_stock:
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
        self.buy_dialog.open()

    async def _load_available_buys(self):
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
            self.buy_dialog.close()

    async def refresh_chip_stock_list(self):
        df = self.service.get_chip_summary("")
        df = df[df["hold_volume"] != 0].drop_duplicates("name")
        names = sorted(df["name"].tolist()) if not df.empty else []
        self.chip_price_ui.clear_stock_list()
        for name in names:
            self.chip_price_ui.add_stock_item(name, lambda _, n=name: self._on_stock_click(n))
        if names and not self.current_selected_stock:
            await self._on_stock_click(names[0])

    async def _on_stock_click(self, stock_name):
        self.current_selected_stock = stock_name
        await self.refresh_chip_price()

    async def refresh_chip_price(self):
        if not self.current_selected_stock:
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
        keyword = self.summary_ui.get_search_keyword()
        df = self.service.get_chip_summary(keyword)
        rows = []
        if not df.empty:
            df = df.astype({"total_buy": int, "total_sell": int, "hold_volume": int})
            df["summary_key"] = df["name"]
            rows = df.to_dict("records")
        self.summary_ui.set_rows(rows)

    async def refresh_table(self):
        keyword = self.trade_table_ui.get_search_keyword()
        df = self.service.get_all_trades()
        if keyword:
            df = df[df["name"].str.contains(keyword, na=False)]
        if "time" in df.columns:
            df["time"] = df["time"].astype(str).str.replace("T", " ")
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
            ui.notify("备份并重置成功", type="positive")
            await self.refresh_all_data()
        else:
            ui.notify("重置失败", type="negative")
        dialog.close()

    async def refresh_all_data(self):
        await self.refresh_match_stock_list()
        await self.refresh_sell_match_table()
        await self.refresh_chip_stock_list()
        await self.refresh_chip_price()
        await self.refresh_chip_summary()
        await self.refresh_table()

    def run(self):
        ui.run(title=Config.APP_TITLE, host=Config.HOST, port=Config.PORT, reload=True)
