import sqlite3
import os
import pandas as pd
import shutil
import gc
from datetime import datetime

class TradeDatabase:
    def __init__(self, db_path="data/chip_insight.db"):
        # Ensure directory exists
        self.db_dir = os.path.dirname(db_path)
        if self.db_dir:
            os.makedirs(self.db_dir, exist_ok=True)
            
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    time TEXT,
                    name TEXT,
                    action TEXT,
                    price REAL,
                    volume INTEGER,
                    amount REAL,
                    create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(time, name, action, volume, price) 
                )
            ''')
            conn.commit()

    def save_trades(self, df: pd.DataFrame) -> int:
        if df.empty: 
            return 0
            
        new_rows = 0
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            for _, row in df.iterrows():
                try:
                    # Format time if it is a datetime object
                    trade_time = row['time']
                    if hasattr(trade_time, 'strftime'):
                        trade_time = trade_time.strftime('%Y-%m-%d %H:%M:%S')
                    
                    cursor.execute('''
                        INSERT OR IGNORE INTO trades (time, name, action, price, volume, amount)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (
                        str(trade_time),
                        str(row.get('name', '未知')),
                        str(row.get('action', '未知')),
                        float(row.get('price', 0.0)),
                        int(row.get('volume', 0)),
                        float(row.get('amount', 0.0))
                    ))
                    
                    if cursor.rowcount > 0:
                        new_rows += 1
                except Exception as e:
                    print(f"[DATABASE ERROR] 写入失败: {e}")
            conn.commit()
        return new_rows

    def get_all_trades(self) -> pd.DataFrame:
        try:
            with sqlite3.connect(self.db_path) as conn:
                return pd.read_sql_query("SELECT * FROM trades ORDER BY time DESC", conn)
        except Exception:
            return pd.DataFrame()

    def clear_all_trades(self) -> bool:
        try:
            # Generate backup path
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = f"{self.db_path}.{timestamp}.bak"
            
            if os.path.exists(self.db_path):
                # Use self.db_path directly (self.db does not exist)
                shutil.copy2(self.db_path, backup_path)
                print(f"[DATABASE INFO] 已备份至: {backup_path}")
                
                # Release handles for Windows compatibility
                gc.collect() 

                # Delete original file
                os.remove(self.db_path)
            
            # Re-initialize empty database
            self._init_db()
            print("[DATABASE INFO] 已重置新数据库。")
            return True
            
        except Exception as e:
            print(f"[DATABASE ERROR] 备份并重置失败: {e}")
            return False