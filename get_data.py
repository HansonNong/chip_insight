import akshare as ak
import pandas as pd
import os

def download_stock_60m_with_turnover(symbol, float_shares, period=60, target_dir="./stock_data"):
    os.makedirs(target_dir, exist_ok=True)
    
    code = symbol
    if symbol[:2] not in ["sz", "sh"]:
        code = f"sh{symbol}" if symbol.startswith("6") else f"sz{symbol}"
    
    try:
        print(f"Fetching {period}min data for {code}...")
        df = ak.stock_zh_a_minute(symbol=code, period=str(period), adjust="qfq")
    except Exception as e:
        print(f"Error fetching data: {str(e)}")
        return None

    if df is None or df.empty:
        print("No valid data fetched for the symbol")
        return None

    df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
    df['close'] = pd.to_numeric(df['close'], errors='coerce')
    
    df['turnover_rate'] = df['volume'] / float_shares

    df = df.drop_duplicates(keep="last").sort_index().reset_index(drop=True)
    
    filename = f"{code}_{period}m_with_turnover_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    save_path = os.path.join(target_dir, filename)
    
    df.to_excel(save_path, index=False)
    return save_path

if __name__ == "__main__":
    my_symbol = "603667" 
    my_float_shares = 3.66 * 1e+8
    
    excel_path = download_stock_60m_with_turnover(my_symbol, my_float_shares, period=60)
    
    if excel_path:
        print(f"Data saved successfully: {excel_path}")
    else:
        print("Data download failed.")