import akshare as ak
import pandas as pd
import re

def get_stock_data(symbol: str, period: int = 60) -> tuple[pd.DataFrame | None, str]:
    pure_symbol = re.sub(r"[^0-9]", "", symbol)
    if not pure_symbol:
        return None, ""
    
    if symbol.startswith(("sh", "sz")):
        code = symbol
    else:
        code = f"sh{pure_symbol}" if pure_symbol.startswith("6") else f"sz{pure_symbol}"

    try:
        info_df = ak.stock_individual_info_em(symbol=pure_symbol)
        float_shares = float(info_df[info_df['item'] == '流通股']['value'].values[0])
    except Exception as e:
        print(f"获取流通股失败: {e}")
        return None, code

    try:
        df = ak.stock_zh_a_minute(symbol=code, period=str(period), adjust="qfq")
    except Exception as e:
        print(f"获取K线数据失败: {e}")
        return None, code

    if df is None or df.empty:
        return None, code

    df = df.rename(columns={"datetime": "day"})  # 适配visualize_cost的字段名
    df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
    df['close'] = pd.to_numeric(df['close'], errors='coerce')
    df['turnover_rate'] = df['volume'] / float_shares  # 换手率=成交量/流通股
    df = df.drop_duplicates(keep="last").sort_index().reset_index(drop=True)
    
    return df, code


if __name__ == "__main__":
    my_symbol = "603667" 
    target_dir="./cache"
    period=60
    import os

    df, code = get_stock_data(my_symbol, period=period)

    filename = f"{code}_{period}m_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    save_path = os.path.join(target_dir, filename)

    if df is not None:
        df.to_excel(save_path, index=False)
        print(f"Data saved successfully: {save_path}")
    else:
        print("No data to save.")