import pandas as pd
from typing import Any, cast
from datetime import datetime

from db.database import TradeDatabase
from core.parse_input import TradeImageParser
from core.fetch_data import get_all_a_shares_map


class TradeService:
    def __init__(self, db: TradeDatabase, parser: TradeImageParser) -> None:
        """Initialize service with database and parser instances."""
        self.db: TradeDatabase = db
        self.parser: TradeImageParser = parser

    def parse_image(self, img_bytes: bytes) -> pd.DataFrame:
        """Parse trade record information from image bytes."""
        return cast(pd.DataFrame, self.parser.parse(img_bytes))

    def save_trades(self, df: pd.DataFrame) -> int:
        """Complete missing stock codes and save trades to database."""
        if df.empty:
            return 0
        
        # Split large volume trades into multiple records of 100 shares each, with adjusted timestamps for uniqueness
        split_rows = []
        row_idx = 0
        for _, row in df.iterrows():
            row_idx += 1
            try:
                vol = int(row.get('volume', 0))
            except (ValueError, TypeError):
                vol = 0
                
            if vol > 0:
                price = float(row.get('price', 0.0))
                chunks = vol // 100
                rem = vol % 100
                
                orig_time = str(row.get('time', ''))
                
                for i in range(chunks):
                    new_row = row.copy().to_dict()
                    new_row['volume'] = 100
                    new_row['amount'] = round(price * 100, 2)
                    if orig_time and orig_time != 'nan':
                        new_row['time'] = f"{orig_time}.{row_idx:03d}{i:03d}"
                    split_rows.append(new_row)
                    
                if rem > 0:
                    new_row = row.copy().to_dict()
                    new_row['volume'] = rem
                    new_row['amount'] = round(price * rem, 2)
                    if orig_time and orig_time != 'nan':
                        new_row['time'] = f"{orig_time}.{row_idx:03d}{chunks:03d}"
                    split_rows.append(new_row)
            else:
                split_rows.append(row.to_dict())
                
        df = pd.DataFrame(split_rows)

        # Fetch mapping table for stock names and codes
        mapping: dict[str, str] | None = get_all_a_shares_map("./db")
        
        if mapping:
            def fill_func(row: pd.Series) -> str:
                code = str(row.get('code', '')).strip()
                name = str(row.get('name', ''))
                # Try matching by name if code is invalid or missing
                if not code or code == 'nan' or len(code) < 6:
                    return mapping.get(name, code)
                return code

            df['code'] = df.apply(fill_func, axis=1)

        return int(self.db.save_trades(df))

    def backup_and_clear(self) -> bool:
        """Backup current data and clear the trade database."""
        return bool(self.db.clear_all_trades())

    def calc_annual(
        self, 
        buy_time_str: str, 
        sell_time_str: str, 
        profit_pct: float
    ) -> float:
        """Calculate annualized return based on holding period."""
        try:
            bt = pd.to_datetime(buy_time_str).date()
            st = pd.to_datetime(sell_time_str).date()
            days = max((st - bt).days, 1)
            
            return float(profit_pct) * (365 / days)
                        
        except Exception:
            return 0.0

    def remove_match(self, sell_id: str) -> bool:
        """Remove existing match for a specific sell record."""
        return bool(self.db.remove_sell_buy_match(sell_id))

    def create_match(self, sell_id: str, buy_id: str) -> bool:
        """Create a new match between a sell and a buy record."""
        return bool(self.db.create_sell_buy_match(sell_id, buy_id))

    def get_available_buys(
        self, 
        stock_name: str, 
        sell_id: str | None = None
    ) -> pd.DataFrame:
        """Retrieve buy records available for matching."""
        return cast(
            pd.DataFrame, 
            self.db.get_available_buys_for_match(stock_name, sell_id)
        )

    def get_sell_records_with_match(self, stock_name: str) -> pd.DataFrame:
        """Fetch sell records along with their matching status."""
        return cast(pd.DataFrame, self.db.get_sell_records_with_match(stock_name))

    def get_chip_summary(self, keyword: str = "") -> pd.DataFrame:
        """Get summary of stock holdings filtered by keyword."""
        return cast(pd.DataFrame, self.db.get_chip_summary(keyword))

    def get_enriched_chip_summary(self, keyword: str = "") -> pd.DataFrame:
        """Get chip summary and enrich it with calculated net profit for each stock."""
        df = self.get_chip_summary(keyword)
        if df.empty:
            return df

        net_profits = []
        for name in df["name"]:
            sell_df = self.get_sell_records_with_match(name)
            stock_profit = 0.0
            if not sell_df.empty and "match_status" in sell_df.columns and "profit" in sell_df.columns:
                matched = sell_df[sell_df["match_status"] == "已匹配"]
                stock_profit = matched["profit"].sum()
            net_profits.append(round(stock_profit, 2))
        df["net_profit"] = net_profits
        return df

    def get_holding_chips(self, stock_name: str) -> pd.DataFrame:
        """Fetch holding chips for a specific stock, accounting for matched sells."""
        return cast(pd.DataFrame, self.db.get_holding_chips(stock_name))

    def get_chip_price(self, stock_name: str) -> pd.DataFrame:
        """Fetch price distribution for a specific stock."""
        return cast(pd.DataFrame, self.db.get_chip_price(stock_name))

    def get_all_trades(self) -> pd.DataFrame:
        """Retrieve all trade records from database."""
        return cast(pd.DataFrame, self.db.get_all_trades())
    
    def update_stock_code(self, stock_name: str, code: str) -> bool:
        """Update stock code for a given stock name."""
        return bool(self.db.update_stock_code(stock_name, code))

    def auto_fill_missing_codes(self) -> int:
        """Scan and auto-fill missing stock codes in the database."""
        # Retrieve chip statistics including those with empty codes
        summary_df: pd.DataFrame = self.db.get_chip_summary("")
        if summary_df.empty:
            return 0
            
        # Obtain mapping table
        mapping: dict[str, str] | None = get_all_a_shares_map("./db")
        if not mapping:
            return 0
            
        count = 0
        # Filter names with empty or invalid codes for matching
        for _, row in summary_df.iterrows():
            name = str(row['name'])
            current_code = str(row.get('code', ""))
            
            if not current_code or len(current_code) < 6:
                matched_code = mapping.get(name)
                if matched_code:
                    self.db.update_stock_code(name, matched_code)
                    count += 1
        return count
    
    def delete_combined_trades(self, trade_ids_str: str) -> tuple[bool, str]:
        """Delete a group of combined trades and unbind any matches."""
        trade_ids = [int(x) for x in trade_ids_str.split(',') if x.strip()]
        
        for tid in trade_ids:
            self.db.remove_sell_buy_match(str(tid))
        
            if hasattr(self.db, 'delete_trade'):
                success = self.db.delete_trade(tid)
                if success is False:
                    return False, "底层数据库执行删除记录失败"
        
            else:
                return False, "底层数据库缺少 delete_trade 方法，请在 db/database.py 中补充实现"
        
        return True, ""

    def update_trade_volume(self, trade_ids_str: str, new_volume: int) -> tuple[bool, str]:
        """Update volume for a combined trade, unbind matches, and re-split."""
        if new_volume <= 0:
            return False, "数量不能小于等于 0"
            
        trade_ids = [int(x) for x in trade_ids_str.split(',') if x.strip()]
        if not trade_ids:
            return False, "无有效记录"
            
        base_trade = self.db.get_trade(trade_ids[0])
        if not base_trade:
            return False, "交易记录不存在"
            
        success, msg = self.delete_combined_trades(trade_ids_str)
        if not success:
            return False, f"删除旧记录失败: {msg}"
                
        time_str = str(base_trade.get('time', ''))
        if '.' in time_str:
            time_str = time_str.split('.')[0]
            
        row = {
            'time': time_str,
            'name': base_trade.get('name', ''),
            'code': base_trade.get('code', ''),
            'action': base_trade.get('action', ''),
            'price': base_trade.get('price', 0.0),
            'volume': new_volume
        }
        
        df = pd.DataFrame([row])
        self.save_trades(df)

        return True, ""

    def update_trade_record(self, trade_id: int, field: str, value: Any) -> tuple[bool, str]:
        """Update a single field of a trade record."""
        trade = self.db.get_trade(trade_id)
        if not trade:
            return False, "交易记录不存在"

        updates: dict[str, Any] = {}
        
        if field == 'date' or field == 'time':
            current_time_str = str(trade['time'])
            frac = ""
            if "." in current_time_str:
                parts_frac = current_time_str.split(".", 1)
                current_time_str = parts_frac[0]
                frac = "." + parts_frac[1]

            parts = current_time_str.split(' ')
            date_part = parts[0] if len(parts) > 0 else '1970-01-01'
            time_part = parts[1] if len(parts) > 1 else '00:00:00'

            if field == 'date':
                try:
                    datetime.strptime(str(value).strip(), "%Y-%m-%d")
                    updates['time'] = f"{str(value).strip()} {time_part}{frac}"
                except ValueError:
                    return False, "日期格式错误或不存在，正确格式应为 YYYY-MM-DD"
            else:  # field == 'time'
                try:
                    datetime.strptime(str(value).strip(), "%H:%M:%S")
                    updates['time'] = f"{date_part} {str(value).strip()}{frac}"
                except ValueError:
                    return False, "时间格式错误或不存在，正确格式应为 HH:MM:SS"
        
        elif field == 'price' or field == 'volume':
            if float(value) <= 0:
                return False, f"{'价格' if field == 'price' else '数量'}不能小于等于 0"
            updates[field] = value
            price = float(updates.get('price', trade['price']))
            volume = int(updates.get('volume', trade['volume']))
            updates['amount'] = round(price * volume, 2)
        
        else:  # name
            if not str(value).strip():
                return False, "名称不能为空"
            updates[field] = value

        success = bool(self.db.update_trade(trade_id, updates))
        return success, "" if success else "数据库更新失败"

    def update_float_shares(self, stock_name: str, float_shares: float) -> bool:
        """Update floating shares information for a stock."""
        return bool(self.db.update_float_shares(stock_name, float_shares))