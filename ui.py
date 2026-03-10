from nicegui import ui, events
import pandas as pd
from parse_input import TradeImageParser
import cv2
import numpy as np
from typing import Optional
import traceback


class TradeImageParseUI:
    def __init__(self, y_threshold=20, port=8080, host="0.0.0.0"):
        self.parser = TradeImageParser(y_threshold=y_threshold)
        self.port = port
        self.host = host

        self.tip_label: Optional[ui.label] = None
        self.result_table: Optional[ui.column] = None

        self._init_ui()

    def _init_ui(self):
        ui.page_title("交易记录图片解析工具")
        with ui.card().style("width:90%;max-width:1200px;margin:20px auto;padding:20px;"):
            ui.label("交易记录图片解析").style("font-size:24px;font-weight:bold;margin-bottom:20px;")
            ui.upload(
                label="点击/拖拽上传图片",
                on_upload=self._parse_trade_image,
                max_files=1,
                auto_upload=True
            ).style("margin-bottom:20px;")
            self.tip_label = ui.label("").style("font-size:14px;margin-bottom:15px;")
            self.result_table = ui.column()

    # 统一日志接口
    def log(self, message: str, level: str = "info"):
        self.tip_label.set_text(message)
        level_map = {
            "info": "[INFO]",
            "success": "[SUCCESS]",
            "warn": "[WARNING]",
            "error": "[ERROR]"
        }
        tag = level_map.get(level, "[INFO]")
        print(f"{tag} {message}")

    async def _parse_trade_image(self, e: events.UploadEventArguments):
        if self.result_table:
            self.result_table.clear()

        try:
            # 正确读取图片
            img_bytes = await e.file.read()

            nparr = np.frombuffer(img_bytes, np.uint8)
            img_data = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

            if img_data is None:
                self.log("❌ 不是有效的图片文件", "error")
                return

            df = self.parser.parse(img_bytes)

            if df.empty:
                self.log("❌ 未解析到任何交易记录", "warn")
                return

            if 'time' in df.columns:
                df['time'] = df['time'].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('未知时间')
            df = df.fillna('-')

            self.log(f"✅ 解析成功：共 {len(df)} 条记录", "success")

            with self.result_table:
                # ==================== ✅ 修复表格参数 ====================
                ui.table(
                    columns=[{"name": col, "label": col, "field": col} for col in df.columns],
                    rows=df.to_dict('records'),
                    pagination={"rowsPerPage": 10}
                )
                # ==========================================================

        except Exception as err:
            err_msg = f"❌ 解析失败：{str(err)}"
            self.log(err_msg, "error")
            print("\n" + "="*60)
            traceback.print_exc()
            print("="*60 + "\n")

    def run(self):
        ui.run(title="交易记录解析工具", port=self.port, host=self.host, reload=True)


if __name__ in {"__main__", "__mp_main__"}:
    app = TradeImageParseUI()
    app.run()