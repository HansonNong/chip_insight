import sqlite3
import os
import pandas as pd
import shutil
import gc
from datetime import datetime

class TradeDatabase:
    def __init__(self, db_path: str = "db/data/chip_insight.db"):
        self.db_dir = os.path.dirname(db_path)
        if self.db_dir:
            os.makedirs(self.db_dir, exist_ok=True)
        self.db_path = db_path
        self._init_db()

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            # 原有交易表
            conn.execute('''
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    time TEXT,
                    name TEXT,
                    code TEXT DEFAULT '',
                    action TEXT,
                    price REAL,
                    volume INTEGER,
                    amount REAL,
                    create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(time, name, action, volume, price) 
                )
            ''')
            # === 卖出买入匹配表 ===
            conn.execute('''
                CREATE TABLE IF NOT EXISTS trade_matches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sell_id INTEGER,          -- 卖出记录ID
                    buy_id INTEGER,           -- 对应的买入记录ID
                    match_volume INTEGER,     -- 本次匹配数量
                    matched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (sell_id) REFERENCES trades(id),
                    FOREIGN KEY (buy_id) REFERENCES trades(id)
                )
            ''')
            conn.commit()

    # ====================== 原有方法不变 ======================
    def save_trades(self, df: pd.DataFrame) -> int:
        if df.empty: 
            return 0
        new_rows = 0
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            for _, row in df.iterrows():
                try:
                    trade_time = row['time']
                    if hasattr(trade_time, 'strftime'):
                        trade_time = trade_time.strftime('%Y-%m-%d %H:%M:%S')
                    cursor.execute('''
                        INSERT OR IGNORE INTO trades 
                        (time, name, code, action, price, volume, amount)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        str(trade_time),
                        str(row.get('name', '未知')),
                        str(row.get('code', '')),  # 股票代码
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
                return pd.read_sql_query(
                    "SELECT * FROM trades ORDER BY time DESC", 
                    conn
                )
        except Exception:
            return pd.DataFrame()

    def clear_all_trades(self) -> bool:
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = f"{self.db_path}.{timestamp}.bak"
            if os.path.exists(self.db_path):
                shutil.copy2(self.db_path, backup_path)
                print(f"[DATABASE INFO] 已备份至: {backup_path}")
                gc.collect()
                os.remove(self.db_path)
            self._init_db()
            print("[DATABASE INFO] 已重置新数据库。")
            return True
        except Exception as e:
            print(f"[DATABASE ERROR] 备份并重置失败: {e}")
            return False

    def get_chip_price(self, stock_name: str | None = None) -> pd.DataFrame:
        try:
            with sqlite3.connect(self.db_path) as conn:
                query = '''
                    SELECT 
                        name,
                        price,
                        SUM(CASE WHEN action = '买入' THEN volume ELSE 0 END) AS buy_volume,
                        SUM(CASE WHEN action = '卖出' THEN volume ELSE 0 END) AS sell_volume
                    FROM trades 
                '''
                params = []
                if stock_name and stock_name.strip():
                    query += " WHERE name LIKE ?"
                    params.append(f"%{stock_name.strip()}%")
                query += " GROUP BY name, price ORDER BY name, price"
                df = pd.read_sql_query(query, conn, params=params)
                return df
        except Exception as e:
            print(f"[ERROR] 筹码价格查询失败: {e}")
            return pd.DataFrame()

    def get_chip_summary(self, stock_name: str | None = None) -> pd.DataFrame:
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("PRAGMA table_info(trades)")
                columns = [col[1] for col in cursor.fetchall()]
                if "code" not in columns:
                    cursor.execute("ALTER TABLE trades ADD COLUMN code TEXT DEFAULT ''")
                    conn.commit()

                query = '''
                    SELECT 
                        name,
                        COALESCE(MAX(code), '') as code,
                        SUM(CASE WHEN action = '买入' THEN volume ELSE 0 END) AS total_buy,
                        SUM(CASE WHEN action = '卖出' THEN volume ELSE 0 END) AS total_sell,
                        (SUM(CASE WHEN action = '买入' THEN volume ELSE 0 END) 
                        - SUM(CASE WHEN action = '卖出' THEN volume ELSE 0 END)) AS hold_volume
                    FROM trades
                '''
                params = []
                if stock_name and stock_name.strip():
                    query += " WHERE name LIKE ?"
                    params.append(f"%{stock_name.strip()}%")
                query += " GROUP BY name ORDER BY name"

                df = pd.read_sql_query(query, conn, params=params)
                return df
            
        except Exception as e:
            print(f"[ERROR] 筹码统计查询失败: {e}")
            return pd.DataFrame(columns=["name", "code", "total_buy", "total_sell", "hold_volume"])

    # ====================== 【新增】解绑方法：remove_sell_buy_match ======================
    def remove_sell_buy_match(self, sell_id):
        """删除该卖出单的所有匹配，释放筹码可重新匹配"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('DELETE FROM trade_matches WHERE sell_id = ?', (int(sell_id),))
                conn.commit()
                return True
        except Exception as e:
            print(f"[ERROR] 解绑失败: {e}")
            return False

    # ====================== 卖出匹配核心方法 ======================
    def get_available_buys_for_match(self, stock_name):
        """获取某股票可用于匹配的买入（未卖完，含当天）"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                sql = '''
                    WITH buy_matched AS (
                        SELECT buy_id, SUM(match_volume) AS matched
                        FROM trade_matches
                        GROUP BY buy_id
                    )
                    SELECT 
                        t.id,
                        t.time,
                        t.name,
                        t.price,
                        t.volume,
                        (t.volume - IFNULL(bm.matched, 0)) AS remain_volume
                    FROM trades t
                    LEFT JOIN buy_matched bm ON t.id = bm.buy_id
                    WHERE t.action = '买入'
                      AND t.name = ?
                      AND (t.volume - IFNULL(bm.matched, 0)) > 0
                    ORDER BY t.time ASC
                '''
                df = pd.read_sql_query(sql, conn, params=(stock_name,))
                return df
        except Exception as e:
            print(f"[ERROR] 可匹配买入查询失败: {e}")
            return pd.DataFrame()

    def get_sell_records_with_match(self, stock_name):
        """获取卖出记录 + 已匹配盈亏 + 状态"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                sql = '''
                    WITH sell_matched AS (
                        SELECT sell_id, SUM(match_volume) AS matched
                        FROM trade_matches
                        GROUP BY sell_id
                    )
                    SELECT 
                        t.id,
                        t.time,
                        t.name,
                        t.price AS sell_price,
                        t.volume AS sell_volume,
                        IFNULL(sm.matched, 0) AS matched_volume,
                        (t.volume - IFNULL(sm.matched, 0)) AS unmatch_volume,
                        CASE WHEN (t.volume - IFNULL(sm.matched, 0)) = 0 
                             THEN '已匹配' ELSE '未匹配' END AS match_status,
                        -- 平均成本（加权）
                        (SELECT SUM(b.price * tm.match_volume) / SUM(tm.match_volume)
                         FROM trade_matches tm
                         JOIN trades b ON tm.buy_id = b.id
                         WHERE tm.sell_id = t.id) AS avg_buy_price
                    FROM trades t
                    LEFT JOIN sell_matched sm ON t.id = sm.sell_id
                    WHERE t.action = '卖出' AND t.name = ?
                    ORDER BY t.time DESC
                '''
                df = pd.read_sql_query(sql, conn, params=(stock_name,))
                
                # 计算盈利、盈利率
                df['profit'] = 0.0
                df['profit_pct'] = 0.0
                df['buy_time'] = ''
                for i, row in df.iterrows():
                    ap = row['avg_buy_price']
                    sp = row['sell_price']
                    vol = row['matched_volume']
                    if pd.notna(ap) and ap > 0 and vol > 0:
                        df.at[i, 'profit'] = (sp - ap) * vol
                        df.at[i, 'profit_pct'] = (sp - ap) / ap
                return df
        except Exception as e:
            print(f"[ERROR] 卖出匹配记录查询失败: {e}")
            return pd.DataFrame()

    def create_sell_buy_match(self, sell_id, buy_id):
        """建立一笔卖出 ↔ 买入的匹配关系（自动取最小可匹配量）"""
        try:
            sell_id = int(sell_id)
            buy_id = int(buy_id)
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                # 卖出剩余量
                cursor.execute('''
                    SELECT t.volume - IFNULL(SUM(tm.match_volume),0)
                    FROM trades t
                    LEFT JOIN trade_matches tm ON t.id = tm.sell_id
                    WHERE t.id = ?
                ''', (sell_id,))
                sell_remain = cursor.fetchone()[0] or 0
                # 买入剩余量
                cursor.execute('''
                    SELECT t.volume - IFNULL(SUM(tm.match_volume),0)
                    FROM trades t
                    LEFT JOIN trade_matches tm ON t.id = tm.buy_id
                    WHERE t.id = ?
                ''', (buy_id,))
                buy_remain = cursor.fetchone()[0] or 0
                match_vol = min(sell_remain, buy_remain)
                if match_vol <= 0:
                    return False
                # 写入匹配
                cursor.execute('''
                    INSERT INTO trade_matches (sell_id, buy_id, match_volume)
                    VALUES (?, ?, ?)
                ''', (sell_id, buy_id, match_vol))
                conn.commit()
                return True
        except Exception as e:
            print(f"[ERROR] 匹配失败: {e}")
            return False
        
    def update_stock_code(self, stock_name: str, code: str) -> bool:
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE trades 
                    SET code = ? 
                    WHERE name = ?
                ''', (code.strip(), stock_name))
                conn.commit()
                return cursor.rowcount > 0
        except Exception as e:
            print(f"[ERROR] 更新股票代码失败: {e}")
            return False