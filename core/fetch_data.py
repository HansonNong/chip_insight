from datetime import datetime, timedelta
from collections.abc import Callable
from typing import Any
import akshare as ak
import pandas as pd
import threading
import json
import re
import os


def timeout_handler(seconds: int) -> Callable:
    """Decorator to enforce timeout on function execution."""
    def decorator(func: Callable) -> Callable:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            res = [Exception(f"Function {func.__name__} timed out after {seconds}s")]

            def target() -> None:
                try:
                    res[0] = func(*args, **kwargs)
                except Exception as e:
                    res[0] = e
            
            thread = threading.Thread(target=target)
            thread.daemon = True
            thread.start()
            thread.join(seconds)
            
            if thread.is_alive():
                print(f"警告: {func.__name__} 执行超时！")
                return None, "" 
                
            if isinstance(res[0], Exception):
                raise res[0]
            return res[0]
            
        return wrapper
    return decorator


@timeout_handler(seconds=10)
def get_stock_data(
    symbol: str,
    period: int = 60, 
    manual_float_shares: float = 0
) -> tuple[pd.DataFrame | None, str]:
    """Fetch market data and calculate turnover rates."""
    pure_symbol = re.sub(r"[^0-9]", "", symbol)
    if not pure_symbol:
        return None, ""
    
    # Handle market prefix
    if symbol.startswith(("sh", "sz")):
        code = symbol
    else:
        code = f"sh{pure_symbol}" if symbol.startswith("6") else f"sz{pure_symbol}"

    # Get float shares for turnover calculation
    if manual_float_shares > 0:
        float_shares = manual_float_shares * 100000000 
    else:
        try:
            info_df = ak.stock_individual_info_em(symbol=pure_symbol)
            float_shares = float(info_df[info_df['item'] == '流通股']['value'].values[0])
        except Exception as e:
            print(f"获取流通股失败: {e}")
            return None, code

   # Fetch minute-level K-line data
    try:
        df = ak.stock_zh_a_minute(symbol=code, period=str(period), adjust="qfq")
    except Exception as e:
        print(f"获取K线数据失败: {e}")
        return None, code

    if df is None or df.empty:
        return None, code

    # Data transformation and cleaning
    df = df.rename(columns={"datetime": "day"})
    df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
    df['close'] = pd.to_numeric(df['close'], errors='coerce')
    
    # Calculate turnover rate
    df['turnover_rate'] = df['volume'] / float_shares
    df = df.drop_duplicates(keep="last").sort_index().reset_index(drop=True)

    return df, code


def get_stock_data_cached(
    symbol: str,
    period: int = 60,
    manual_float_shares: float = 0,
    cache_dir: str = "./cache",
) -> tuple[pd.DataFrame | None, str]:
    """Fetch market data with caching mechanism."""
    pure_symbol = re.sub(r"[^0-9]", "", symbol)
    if not pure_symbol:
        return None, ""

    code = f"sh{pure_symbol}" if symbol.startswith("6") else f"sz{pure_symbol}"
    cache_file = os.path.join(cache_dir, f"{code}_{period}m.parquet")

    # Check if cache directory exists and create if not
    os.makedirs(cache_dir, exist_ok=True)

    # Check if cache file exists and is recent
    if os.path.exists(cache_file):
        file_modified_time = datetime.fromtimestamp(os.path.getmtime(cache_file))
        cache_valid_until = file_modified_time + timedelta(hours=1)

        if datetime.now() <= cache_valid_until:
            try:
                df = pd.read_parquet(cache_file)
                print(f"Loaded K-line data for {symbol} from cache.")
                return df, code
            except Exception as e:
                print(f"Error loading cached data: {e}")
                os.remove(cache_file)  # Remove corrupted cache file
        else:
            print(f"Cache for {symbol} expired, fetching new data.")

    # If cache doesn't exist or is outdated, fetch new data
    df, code = get_stock_data(symbol, period, manual_float_shares)

    if df is not None and not df.empty:
        try:
            df.to_parquet(cache_file)
            print(f"Saved K-line data for {symbol} to cache.")
        except Exception as e:
            print(f"Error saving data to cache: {e}")

    return df, code



def get_cache_info(symbol: str, period: int = 60, cache_dir: str = "./cache") -> str | None:
    """Return cache modification time if valid, else None."""
    pure_symbol = re.sub(r"[^0-9]", "", symbol)
    if not pure_symbol:
        return None
    code = f"sh{pure_symbol}" if symbol.startswith("6") else f"sz{pure_symbol}"
    cache_file = os.path.join(cache_dir, f"{code}_{period}m.parquet")
    
    if os.path.exists(cache_file):
        mtime = datetime.fromtimestamp(os.path.getmtime(cache_file))
        if datetime.now() <= mtime + timedelta(hours=1):
            return mtime.strftime('%Y-%m-%d %H:%M:%S')
    return None


def get_all_a_shares_map(cache_dir: str = "./db") -> dict[str, str]:
    """Get mapping of stock names to codes with local cache."""
    cache_path = os.path.join(cache_dir, "stock_mapping_cache.json")
    
    # Check cache validity
    if os.path.exists(cache_path):
        mtime = datetime.fromtimestamp(os.path.getmtime(cache_path))
        if datetime.now() - mtime < timedelta(days=1):
            with open(cache_path, 'r', encoding='utf-8') as f:
                return json.load(f)

    # Refresh data from akshare
    try:
        print("正在从 akshare 更新全量股票映射表...")
        df = ak.stock_info_a_code_name()
        mapping = dict(zip(df['name'], df['code']))
        
        os.makedirs(cache_dir, exist_ok=True)
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump(mapping, f, ensure_ascii=False, indent=2)
            
        return mapping
    except Exception as e:
        print(f"更新映射表失败: {e}")
        return {}

    

if __name__ == "__main__":
    my_symbol = "603667" 
    target_dir = "./cache"
    period = 60

    df, code = get_stock_data(my_symbol, period=period)

    if df is not None:
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)
            
        timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{code}_{period}m_{timestamp}.xlsx"
        save_path = os.path.join(target_dir, filename)
        
        df.to_excel(save_path, index=False)
        print(f"Data saved successfully: {save_path}")
    else:
        print("No data to save.")