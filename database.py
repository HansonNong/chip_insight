import sqlite3
import os
import pandas as pd
import shutil
from datetime import datetime

class TradeDatabase:
    def __init__(self, db_path="data/chip_insight.db"):
        # 确保数据库存放目录存在
        self.db_dir = os.path.dirname(db_path)
        if self.db_dir:
            os.makedirs(self.db_dir, exist_ok=True)
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """初始化数据库表结构"""
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
        if df.empty: return 0
        new_rows = 0
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            for _, row in df.iterrows():
                try:
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
        """
        清空逻辑：先备份当前数据库，再彻底重置
        """
        try:
            # 1. 生成时间戳
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = f"{self.db_path}.{timestamp}.bak"
            
            # --- 核心修正点：确保文件句柄释放 ---
            # 在 Windows 下，必须确保没有任何连接指向该文件才能执行 os.remove
            # 如果你有全局的 self.conn，请在这里执行 self.conn.close()
            # ----------------------------------

            if os.path.exists(self.db_path):
                # 2. 执行备份
                # 使用 shutil.copy2 即使文件被读取锁定通常也能成功，
                # 但接下来的 os.remove 必须要求文件完全没被占用
                shutil.copy2(self.db.path if hasattr(self, 'db') else self.db_path, backup_path)
                print(f"[DATABASE INFO] 已备份至: {backup_path}")
                
                # 3. 尝试强制触发垃圾回收（可选，但在处理文件锁时有时有效）
                import gc
                gc.collect() 

                # 4. 删除原文件
                # 如果这里报错，说明程序中还有其他地方持有了 sqlite3.connect 且未关闭
                os.remove(self.db_path)
            
            # 5. 重新初始化
            self._init_db()
            print("[DATABASE INFO] 已重置新数据库。")
            return True
        except Exception as e:
            print(f"[DATABASE ERROR] 备份并重置失败: {e}")
            return False