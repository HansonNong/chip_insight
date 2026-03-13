import pandas as pd

from db.database import TradeDatabase
from core.parse_input import TradeImageParser
from core.fetch_data import get_all_a_shares_map


class TradeService:
    def __init__(self, db: TradeDatabase, parser: TradeImageParser):
        self.db = db
        self.parser = parser

    def parse_image(self, img_bytes):
        return self.parser.parse(img_bytes)

    def save_trades(self, df):
        if df.empty:
            return 0
        
        # 1. 获取全量映射表 (利用你已经写好的 get_all_a_shares_map)
        mapping = get_all_a_shares_map("./db")
        
        if mapping:
            def fill_func(row):
                code = str(row.get('code', '')).strip()
                name = row.get('name', '')
                # 如果代码无效（为空或长度不足6位），尝试根据名称匹配
                if not code or code == 'nan' or len(code) < 6:
                    return mapping.get(name, code) # 匹配不到则保留原样
                return code

            # 应用补全逻辑
            df['code'] = df.apply(fill_func, axis=1)

        # 2. 执行真正的保存动作
        return self.db.save_trades(df)

    def backup_and_clear(self):
        return self.db.clear_all_trades()

    def calc_annual(self, buy_time_str, sell_time_str, profit_pct) -> float:
        try:
            bt = pd.to_datetime(buy_time_str).date()
            st = pd.to_datetime(sell_time_str).date()
            days = max((st - bt).days, 1)
            
            val = float(profit_pct) * (365 / days)
            return val
                        
        except Exception:
            return 0

    def remove_match(self, sell_id):
        return self.db.remove_sell_buy_match(sell_id)

    def create_match(self, sell_id, buy_id):
        return self.db.create_sell_buy_match(sell_id, buy_id)

    def get_available_buys(self, stock_name, sell_id=None):
        return self.db.get_available_buys_for_match(stock_name, sell_id)

    def get_sell_records_with_match(self, stock_name):
        return self.db.get_sell_records_with_match(stock_name)

    def get_chip_summary(self, keyword=""):
        return self.db.get_chip_summary(keyword)

    def get_chip_price(self, stock_name):
        return self.db.get_chip_price(stock_name)

    def get_all_trades(self):
        return self.db.get_all_trades()
    
    def update_stock_code(self, stock_name: str, code: str):
        return self.db.update_stock_code(stock_name, code)

    def auto_fill_missing_codes(self) -> int:
        """
        扫描数据库中代码为空的股票，并尝试自动补全
        返回成功补全的数量
        """
        # 1. 获取所有筹码统计（包含代码为空的）
        summary_df = self.db.get_chip_summary("")
        if summary_df.empty:
            return 0
            
        # 2. 获取映射表
        mapping = get_all_a_shares_map("./db")
        if not mapping:
            return 0
            
        count = 0
        # 3. 筛选出代码为空或无效的名称
        for _, row in summary_df.iterrows():
            name = row['name']
            current_code = row.get('code', "")
            
            if not current_code or len(str(current_code)) < 6:
                # 尝试匹配
                matched_code = mapping.get(name)
                if matched_code:
                    self.db.update_stock_code(name, matched_code)
                    count += 1
        return count