import asyncio
from typing import Any, cast

import plotly.graph_objects as go
from nicegui import Client, events, ui
import pandas as pd

from core.fetch_data import get_stock_data
from core.parse_input import TradeImageParser
from core.visualize_cost import ChipDistVisualizer
from db.database import TradeDatabase

from .components import (
    BuyMatchDialogUI, ChipPriceUI, HeaderUI,
    SellMatchUI, SummaryUI, TradeTableUI, UploadCardUI, StockSelectorUI
)
from .config import Config
from .logger import AppLogger
from .service import TradeService


class ChipInSightApp:
    def __init__(self) -> None:
        """Initialize core services and UI components."""
        self.logger: AppLogger = AppLogger()
        self.db: TradeDatabase = TradeDatabase()
        self.parser: TradeImageParser = TradeImageParser()
        self.service: TradeService = TradeService(self.db, self.parser)

        self.current_selected_stock: str = ""
        self.current_matching_sell_id: str = ""
        self.chip_visualizer: ChipDistVisualizer = ChipDistVisualizer(
            bin_count=300,
            decay_threshold=1e-3,
            smoothing=1.5,
            contrast=3
        )

        self.header: HeaderUI | None = None
        self.upload_card: UploadCardUI | None = None
        self.stock_selector_ui: StockSelectorUI | None = None
        self.sell_match_ui: SellMatchUI | None = None
        self.chip_price_ui: ChipPriceUI | None = None
        self.summary_ui: SummaryUI | None = None
        self.trade_table_ui: TradeTableUI | None = None
        self.buy_dialog: BuyMatchDialogUI | None = None


    def _build_ui(self) -> None:
        """Construct the UI layout and bind events."""
        ui.page_title(Config.APP_TITLE)
        self.header = HeaderUI(
            on_refresh=self.refresh_all_data,
            on_clear=self._confirm_clear
        )

        ui.add_head_html(
            '<meta name="viewport" content="width=device-width, '
            'initial-scale=0.6, maximum-scale=1.0">'
        )

        container_style = (
            "w-full mx-auto my-1 px-1 sm:px-2 "
            "min-h-[80vh] sm:max-w-6xl"
        )
        with ui.column().classes(container_style):
            self.upload_card = UploadCardUI(on_multi_upload=self._parse_trade_images_batch)

            if self.upload_card and self.upload_card.tip_label:
                self.logger.set_tip_label(self.upload_card.tip_label)

            self.stock_selector_ui = StockSelectorUI(on_change=self._on_stock_switch)

            self.sell_match_ui = SellMatchUI(
                on_row_click=self._open_match_dialog
            )
            self.chip_price_ui = ChipPriceUI(
                on_gen_plot=lambda: self.refresh_chip_dist_plot(auto_popup=True)
            )

            self.summary_ui = SummaryUI(
                on_search=self.refresh_chip_summary,
                on_code_edit=self._edit_stock_code,
                on_auto_fill=self._handle_auto_fill
            )

            if (s_ui := self.summary_ui):
                s_ui.chip_summary_table.on( # type: ignore
                    "edit_code",
                    lambda e: self._edit_stock_code(e.args) 
                )
                s_ui.chip_summary_table.on( # type: ignore
                    "edit_float",
                    lambda e: self._edit_float_shares(e.args) 
                )

            self.trade_table_ui = TradeTableUI(on_search=self.refresh_table)
            if self.trade_table_ui.table:
                self.trade_table_ui.table.on(
                    'update_trade', 
                    lambda e: self._handle_trade_update(e.args)
                )
                self.trade_table_ui.table.on(
                    'delete_trade',
                    lambda e: self._handle_trade_delete(e.args)
                )

        self.buy_dialog = BuyMatchDialogUI(on_match=self._do_match)

    async def refresh_global_stock_list(self) -> None:
        """Update available stocks for the global selector."""
        df = await asyncio.to_thread(self.service.get_chip_summary, "")
        df = df[df["hold_volume"] != 0].drop_duplicates("name")
        names: list[str] = sorted(df["name"].tolist()) if not df.empty else []

        if self.stock_selector_ui:
            self.stock_selector_ui.set_options(names)
            if not names:
                self.current_selected_stock = ""
                self.stock_selector_ui.set_value("")
            elif not self.current_selected_stock or self.current_selected_stock not in names:
                self.stock_selector_ui.set_value(names[0])
            else:
                self.stock_selector_ui.set_value(self.current_selected_stock)

    async def _on_stock_switch(self, e: Any) -> None:
        """Switch current stock context."""
        stock_name = e.value if hasattr(e, 'value') else e
        if not stock_name:
            return
        if stock_name == self.current_selected_stock:
            return
            
        self.current_selected_stock = str(stock_name)
        await self.refresh_sell_match_table()
        await self.refresh_chip_price()

    async def refresh_sell_match_table(self) -> None:
        """Fetch and display sell records with match status."""
        if not self.current_selected_stock or not self.sell_match_ui:
            return

        df = await asyncio.to_thread(
            self.service.get_sell_records_with_match, self.current_selected_stock
        )
        rows: list[dict[str, Any]] = []

        if not df.empty:
            df["time"] = df["time"].astype(str).str.replace("T", " ")
            df["time"] = df["time"].str.split(".").str[0]
            for _, r in df.iterrows():
                profit: float = round(r.get("profit", 0), 2)
                profit_pct: float = r.get("profit_pct", 0)
                buy_time = r.get("buy_time")
                status: str = r.get("match_status", "未匹配")

                annual: float = 0.0
                if status == "已匹配" and buy_time:
                    annual = self.service.calc_annual(buy_time, r["time"], profit_pct)

                rows.append({
                    "sell_id": str(r["id"]),
                    "time": r["time"],
                    "price": round(r["sell_price"], 2),
                    "volume": int(r["sell_volume"]),
                    "profit": profit,
                    "profit_pct": profit_pct,
                    "annual": annual,
                    "status": status,
                })
        self.sell_match_ui.set_rows(rows)

    async def _open_match_dialog(self, e: events.GenericEventArguments) -> None:
        """Open match dialog for a specific row."""
        row: dict[str, Any] = e.args[1]
        self.current_matching_sell_id = str(row["sell_id"])
        await self._load_available_buys()
        if self.buy_dialog:
            self.buy_dialog.open()

    async def _load_available_buys(self) -> None:
        """Load buy orders available for matching."""
        if not self.buy_dialog:
            return

        df = self.service.get_available_buys(
            self.current_selected_stock,
            self.current_matching_sell_id
        )
        rows: list[dict[str, Any]] = []

        if not df.empty:
            df["time"] = df["time"].astype(str).str.replace("T", " ")
            df["time"] = df["time"].str.split(".").str[0]
            for _, r in df.iterrows():
                rows.append({
                    "buy_id": str(r["id"]),
                    "time": r["time"],
                    "price": round(r["price"], 2),
                    "display_vol": f"{int(r['total_remain'])} / {int(r['volume'])}",
                    "status": "已选择" if r["current_matched_vol"] > 0 else "未选择",
                    "is_matched": r["current_matched_vol"] > 0
                })
        self.buy_dialog.set_rows(rows)

    async def _do_match(self, e: Any) -> None:
        """Handle match/unmatch logic."""
        try:
            raw_data = e.args if hasattr(e, 'args') else e
            if isinstance(raw_data, list) and len(raw_data) > 0:
                buy_row = cast(dict[str, Any], raw_data[0])
            elif isinstance(raw_data, dict):
                buy_row = cast(dict[str, Any], raw_data)
            else:
                return

            buy_id = buy_row.get("buy_id")
            is_matched = bool(
                buy_row.get("is_matched", False) or
                buy_row.get("status") == "已选择"
            )

            if not buy_id or not self.current_matching_sell_id:
                ui.notify(
                    f"参数缺失: buy_id={buy_id}, sell_id={self.current_matching_sell_id}",
                    type='negative',
                    position="left"
                )
                return

            if is_matched:
                self.db.delete_specific_match(self.current_matching_sell_id, buy_id)
                ui.notify("已撤销匹配", color='orange', position="left")
            else:
                self.service.remove_match(self.current_matching_sell_id)
                self.service.create_match(self.current_matching_sell_id, buy_id)
                ui.notify("匹配成功", color='positive', position="left")

            await self._load_available_buys()
            await self.refresh_sell_match_table()
            await self.refresh_chip_price()
            await self.refresh_chip_summary()

        except Exception as err:
            print(f"CRITICAL ERROR: {err}")

    async def _handle_trade_update(self, data: dict[str, Any]) -> None:
        """Handle inline editing of a trade record."""
        trade_id_raw = data.get('id')
        field = data.get('field')
        value = data.get('value')

        if trade_id_raw is None or field is None:
            ui.notify("更新失败：参数不完整", type='negative', position='left')
            return

        if field == 'volume':
            self.logger.info(f"正在更新数量 {trade_id_raw} -> {value}")
            await asyncio.sleep(0.01)
            success, msg = await asyncio.to_thread(
                self.service.update_trade_volume, str(trade_id_raw), int(value)
            )
            if success:
                ui.notify("数量更新成功，已重新拆分并解绑匹配", type='positive', position='left')
                await self.refresh_all_data()
            else:
                ui.notify(f"修改失败: {msg}", type='negative', position='left')
                await self.refresh_table()
            return

        trade_ids = str(trade_id_raw).split(',')

        self.logger.info(f"正在更新记录 {trade_id_raw}：字段 {field} -> {value}")
        await asyncio.sleep(0.01)

        all_success = True
        err_msg = ""
        for tid in trade_ids:
            success, msg = await asyncio.to_thread(
                self.service.update_trade_record, int(tid), str(field), value
            )
            if not success:
                all_success = False
                err_msg = msg
                break

        if all_success:
            ui.notify("记录更新成功", type='positive', position='left')
            await self.refresh_all_data()
        else:
            ui.notify(f"修改失败: {err_msg}", type='negative', position='left')
            await self.refresh_table() # Refresh to revert client-side change

    async def _handle_trade_delete(self, trade_ids_raw: Any) -> None:
        """Handle deletion of a combined trade record."""
        trade_ids_str = str(trade_ids_raw)
        
        with ui.dialog() as dialog, ui.card().classes("p-6"):
            ui.label("确定要删除这条交易记录吗？").classes("text-lg font-bold text-red-600")
            ui.label("删除后，该记录及相关匹配将被彻底清除。")

            with ui.row().classes("justify-end gap-2 mt-4"):
                ui.button("取消", on_click=dialog.close).props("outline")
                async def _confirm() -> None:
                    
                    dialog.close()
                    self.logger.info(f"正在删除记录 {trade_ids_str}")
                    success, msg = await asyncio.to_thread(
                        self.service.delete_combined_trades, trade_ids_str
                    )

                    if success:
                        self.logger.info(f"记录已删除 {trade_ids_str}")
                        ui.notify("记录已删除", type='positive', position='left')
                        await self.refresh_all_data()
                    else:
                        self.logger.info(f"删除失败 {trade_ids_str}")
                        ui.notify(f"删除失败: {msg}", type='negative', position='left')

                ui.button("确定", on_click=_confirm, color="red")
        dialog.open()

    async def get_own_chips(self, stock_name: str) -> list[tuple[float, int]]:
        """Calculate net holding volume for each price point, considering matches."""
        df = await asyncio.to_thread(self.service.get_holding_chips, stock_name)
        if df.empty:
            return []

        return list(zip(df["price"].round(2), df["net_volume"].astype(int)))

    async def refresh_chip_dist_plot(self, auto_popup: bool = False) -> None:
        """Update the plotly distribution chart."""
        if not self.current_selected_stock or not self.chip_price_ui:
            if auto_popup:
                ui.notify("请先在列表中选择一支股票", type='warning', position="left")
            return

        summary_df = self.service.get_chip_summary(self.current_selected_stock)
        if summary_df.empty:
            return

        row = summary_df.iloc[0]
        stock_code = str(row.get("code", ""))
        float_shares = float(row.get("float_shares", 0))

        if not stock_code:
            if auto_popup:
                ui.notify(
                    f"{self.current_selected_stock} 未配置代码",
                    type='warning',
                    position="left"
                )
                await self._edit_stock_code(row.to_dict())
            return

        if float_shares <= 0:
            if auto_popup:
                ui.notify(
                    f"请先配置 {self.current_selected_stock} 的自由流通股",
                    type='warning',
                    position="left"
                )
                await self._edit_float_shares(row.to_dict())
            return

        ui.notify(
            f"正在获取 {self.current_selected_stock} 行情数据...",
            position="left",
            duration=1
        )
        await asyncio.sleep(0.1)

        kline_df, _ = await asyncio.to_thread(
            get_stock_data, stock_code, 60, float_shares
        )

        if kline_df is None or kline_df.empty:
            self.chip_price_ui.set_chip_plot(go.Figure())
            ui.notify(
                f"{self.current_selected_stock} 行情数据获取失败",
                type='negative',
                position="left"
            )
            return

        dist_data = await asyncio.to_thread(
            self.chip_visualizer.generate_distribution,
            kline_df
        )
        if not dist_data:
            self.chip_price_ui.set_chip_plot(go.Figure())
            return

        own_chips = await self.get_own_chips(self.current_selected_stock)
        fig = await asyncio.to_thread(
            self.chip_visualizer.render_plot,
            data=dist_data,
            own_chips=own_chips
        )
        self.chip_price_ui.set_chip_plot(fig)

    async def refresh_chip_price(self) -> None:
        """Update the holding price table."""
        if not self.current_selected_stock or not self.chip_price_ui:
            return

        df = await asyncio.to_thread(
            self.service.get_chip_price, self.current_selected_stock
        )
        rows: list[dict[str, Any]] = []
        if not df.empty:
            df = df.astype({"buy_volume": int, "sell_volume": int})
            df["net_volume"] = df["buy_volume"] - df["sell_volume"]
            df["price_key"] = df["name"] + "_" + df["price"].astype(str)
            rows = cast(list[dict[str, Any]], df.to_dict("records"))
        self.chip_price_ui.set_rows(rows)

    async def refresh_chip_summary(self) -> None:
        """Update stock holding summary."""
        if not self.summary_ui:
            return
        keyword = self.summary_ui.get_search_keyword()
        df = await asyncio.to_thread(self.service.get_enriched_chip_summary, keyword)
        rows: list[dict[str, Any]] = []
        if not df.empty:
            df = df.astype({"total_buy": int, "total_sell": int, "hold_volume": int})
            df["summary_key"] = df["name"]
            if "code" not in df.columns:
                df["code"] = ""

            rows = cast(list[dict[str, Any]], df.to_dict("records"))
        self.summary_ui.set_rows(rows)

    async def _edit_stock_code(self, row: dict[str, Any] | Any) -> None:
        """Dialog to update stock code."""
        stock_name: str = str(row["name"])
        current_code: str = str(row.get("code", ""))

        with ui.dialog() as dialog, ui.card().classes("p-6 w-[400px]"):
            ui.label(f"编辑股票代码：{stock_name}").classes("text-lg font-bold mb-4")
            code_input = ui.input(
                label="股票代码",
                value=current_code,
                placeholder="请输入6位数字股票代码"
            ).props("outlined dense").classes("w-full mb-4")

            async def _save() -> None:
                new_code = code_input.value.strip()
                if not new_code.isdigit() or len(new_code) != 6:
                    ui.notify(
                        "股票代码必须是6位数字",
                        type="negative",
                        position="left"
                    )
                    return
                self.service.update_stock_code(stock_name, new_code)
                ui.notify(
                    f"{stock_name} 代码保存成功：{new_code}",
                    type="positive",
                    position="left"
                )
                await self.refresh_chip_summary()
                if self.current_selected_stock == stock_name:
                    await self.refresh_chip_dist_plot(auto_popup=True)
                dialog.close()

            with ui.row().classes("justify-end gap-3 mt-4"):
                ui.button("取消", on_click=dialog.close).props("flat")
                ui.button("保存", on_click=_save, color="blue-5")
        dialog.open()

    async def refresh_table(self) -> None:
        """Update the global transaction table."""
        if not self.trade_table_ui:
            return

        keyword = self.trade_table_ui.get_search_keyword()
        df = await asyncio.to_thread(self.service.get_all_trades)
        if keyword:
            df = df[df["name"].str.contains(keyword, na=False)]
            
        if not df.empty and "time" in df.columns:
            df["time"] = df["time"].astype(str).str.replace("T", " ")
            
            df["_orig_row"] = df["time"].str.extract(r"\.(\d{3})")[0].fillna("000")
            df["time"] = df["time"].str.split(".").str[0]
            
            df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0)
            df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)
            
            group_cols = ["time", "name", "action", "price", "_orig_row"]
            agg_dict = {
                "id": lambda x: ",".join(x.astype(str)),
                "volume": "sum",
                "amount": "sum"
            }
            if "code" in df.columns:
                group_cols.append("code")
                
            df = df.groupby(group_cols, dropna=False, as_index=False).agg(agg_dict)
            df["amount"] = df["amount"].round(2)
            df = df.drop(columns=["_orig_row"], errors="ignore")
            
            parts = df["time"].str.split(n=1, expand=True)
            df["date"] = parts[0] if 0 in parts.columns else ""
            df["time"] = parts[1] if 1 in parts.columns else ""
            df["time"] = df["time"].fillna("")
            
            df = df.sort_values(by=["date", "time"], ascending=[False, False])
        
        elif not df.empty:
            df["date"] = ""
            
        if "code" not in df.columns:
            df["code"] = ""

        rows: list[dict[str, Any]] = cast(
            list[dict[str, Any]],
            df.to_dict("records")
        ) if not df.empty else []
        self.trade_table_ui.set_rows(rows)

    async def _parse_trade_images_batch(self, evt: events.MultiUploadEventArguments) -> None:
        """Process a batch of uploaded trade record images with progress tracking."""
        total_files = len(evt.files)
        if not total_files:
            return

        total_added = 0
        any_success = False

        for i, file_obj in enumerate(evt.files):
            fname = file_obj.name
            progress = f"({i + 1}/{total_files})"
            self.logger.info(f"处理中 {progress}：{fname}")

            # Allow UI to update before blocking operation
            await asyncio.sleep(0.01)

            try:
                img_bytes = await file_obj.read()
                df = await asyncio.to_thread(self.service.parse_image, img_bytes)

                if df.empty:
                    self.logger.error(f"无法识别 {progress}：{fname}")
                    ui.notify(f"无法识别：{fname}", type="negative", position="left")
                    continue

                added = await asyncio.to_thread(self.service.save_trades, df)
                if added > 0:
                    total_added += added
                    any_success = True
                    self.logger.success(f"完成 {progress}：{fname} 新增{added}条记录")
                    ui.notify(f"完成：{fname} 新增{added}条记录", type="positive", position="left")
                else:
                    self.logger.warn(f"跳过重复 {progress}：{fname}")
                    ui.notify(f"跳过重复：{fname}", type="warning", position="left")

            except Exception as e:
                self.logger.error(f"处理失败 {progress}：{fname}，错误：{e}")
                ui.notify(f"处理失败：{fname}", type="negative", position="left")
            finally:
                if hasattr(file_obj, 'close'):
                    file_obj.close() # type: ignore

        if any_success:
            self.logger.success(f"批量处理完成，共新增 {total_added} 条记录。")
            await self.refresh_all_data()
        else:
            self.logger.info("批量处理完成，无新记录添加。")

        if self.upload_card:
            self.upload_card.reset()

    async def _confirm_clear(self) -> None:
        """Show reset confirmation dialog."""
        with ui.dialog() as dialog, ui.card().classes("p-6"):
            ui.label("备份并重置数据库？").classes("text-lg font-bold text-red-600")
            ui.label("当前数据将存为 .bak 文件，系统启用新库")
            with ui.row().classes("justify-end gap-2 mt-4"):
                ui.button("取消", on_click=dialog.close).props("outline")
                ui.button(
                    "确定",
                    on_click=lambda: self._handle_clear(dialog),
                    color="red"
                )
        dialog.open()

    async def _handle_clear(self, dialog: ui.dialog) -> None:
        """Perform database backup and clear."""
        if self.service.backup_and_clear():
            ui.notify("备份并重置成功", type="positive", position="left")
            await self.refresh_all_data()
        else:
            ui.notify("重置失败", type="negative", position="left")
        dialog.close()

    async def _handle_auto_fill(self) -> None:
        """Trigger auto code matching service."""
        ui.notify("正在匹配 A 股代码...", loading=True, position="left")
        count = await asyncio.to_thread(self.service.auto_fill_missing_codes)

        if count > 0:
            ui.notify(
                f"成功自动补全 {count} 个股票代码",
                type="positive",
                position="left"
            )
            await self.refresh_chip_summary()
        else:
            ui.notify(
                "未发现可匹配的代码，请手动填写",
                type="warning",
                position="left"
            )

    async def _edit_float_shares(self, row: dict[str, Any] | Any) -> None:
        """Dialog to update float shares."""
        is_dict = isinstance(row, dict)
        stock_name: str = str(row.get("name") if is_dict else row["name"])
        current_val: float = float(
            row.get("float_shares", 0) if is_dict else row.get("float_shares", 0)
        )

        with ui.dialog() as dialog, ui.card().classes("p-6 w-[350px]"):
            ui.label("编辑自由流通股").classes("text-lg font-bold mb-2")
            ui.label(f"股票名称: {stock_name}").classes("text-grey-7 mb-4")

            val_input = ui.number(
                label="自由流通股本 (单位：亿)",
                value=current_val if current_val > 0 else None,
                format="%.2f",
                precision=2
            ).props("outlined dense autofocus").classes("w-full mb-4")

            async def _save() -> None:
                new_val = val_input.value
                if new_val is None or new_val <= 0:
                    ui.notify(
                        "请输入大于 0 的数值",
                        type="negative",
                        position="left"
                    )
                    return

                if self.service.update_float_shares(stock_name, new_val):
                    ui.notify(
                        f"{stock_name} 自由流通股已更新为 {new_val} 亿",
                        type="positive",
                        position="left"
                    )
                    dialog.close()
                    await self.refresh_chip_summary()
                    if self.current_selected_stock == stock_name:
                        await self.refresh_chip_dist_plot(auto_popup=True)
                else:
                    ui.notify("数据库更新失败", type="negative", position="left")

            with ui.row().classes("justify-end gap-3 mt-4 w-full"):
                ui.button("取消", on_click=dialog.close).props("flat")
                ui.button("保存修改", on_click=_save, color="blue-6")
        dialog.open()

    async def refresh_all_data(self) -> None:
        """Global UI data refresh."""
        await self.refresh_global_stock_list()
        await self.refresh_sell_match_table()
        await self.refresh_chip_price()
        await self.refresh_chip_summary()
        await self.refresh_table()

        if self.current_selected_stock:
            await self.refresh_chip_dist_plot(auto_popup=False)

    def run(self) -> None:
        """Start the application."""
        @ui.page('/')
        async def index_page(client: Client) -> None:
            self._build_ui()
            await client.connected()

            self.logger.info("数据加载中...")
            await asyncio.sleep(0.01)

            await self.refresh_all_data()
            self.logger.success("数据加载完成，系统就绪。")

        ui.run(
            title=Config.APP_TITLE,
            host=Config.HOST,
            port=Config.PORT,
            reload=Config.RELOAD,
            show=Config.SHOW
        )