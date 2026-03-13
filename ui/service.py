import pandas as pd

from db.database import TradeDatabase
from core.parse_input import TradeImageParser

class TradeService:
    def __init__(self, db: TradeDatabase, parser: TradeImageParser):
        self.db = db
        self.parser = parser

    def parse_image(self, img_bytes):
        return self.parser.parse(img_bytes)

    def save_trades(self, df):
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

    def get_available_buys(self, stock_name):
        return self.db.get_available_buys_for_match(stock_name)

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