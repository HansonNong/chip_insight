import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os

def plot_advanced_cyq(file_path):
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return

    df = pd.read_excel(file_path)
    df['day'] = pd.to_datetime(df['day'])
    df = df.sort_values('day').reset_index(drop=True)
    
    min_p, max_p = df['close'].min() * 0.9, df['close'].max() * 1.1
    price_bins = np.linspace(min_p, max_p, 150)
    chips = np.zeros_like(price_bins)
    
    print("Calculating historical chip evolution...")
    for _, row in df.iterrows():
        turnover = row['turnover_rate']
        close_p = row['close']
        
        chips = chips * (1 - turnover)
        idx = np.abs(price_bins - close_p).argmin()
        chips[idx] += turnover

    last_price = df['close'].iloc[-1]
    total_chips = np.sum(chips)
    
    profit_chips = np.sum(chips[price_bins <= last_price])
    profit_ratio = (profit_chips / total_chips) * 100
    hold_up_ratio = 100 - profit_ratio
    avg_price = np.sum(price_bins * chips) / total_chips

    fig, ax1 = plt.subplots(figsize=(12, 7))
    plt.subplots_adjust(right=0.85)

    ax1.plot(df['day'], df['close'], color='blue', label='Close Price', linewidth=1.5, alpha=0.8)
    ax1.set_ylabel('Price (Yuan)')
    ax1.set_xlabel('Date')
    
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax1.xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.xticks(rotation=45)
    ax1.grid(True, linestyle='--', alpha=0.6)

    ax2 = ax1.twiny()
    ax2.barh(price_bins, chips, height=(max_p-min_p)/150, color='orange', alpha=0.6, label='Chips')
    ax2.set_xlabel('Chip Density')
    ax2.set_xticks([]) 

    ax1.axhline(last_price, color='red', linestyle='--', linewidth=1)
    ax1.axhline(avg_price, color='green', linestyle='-.', linewidth=1, label='Avg Price')

    info_text = (
        f"Last Price: {last_price:.2f}\n"
        f"Avg Cost: {avg_price:.2f}\n"
        f"Profit Ratio: {profit_ratio:.1f}%\n"
        f"Hold-up Ratio: {hold_up_ratio:.1f}%"
    )
    plt.text(1.02, 0.8, info_text, transform=ax1.transAxes, fontsize=10,
             verticalalignment='top', bbox=dict(boxstyle='round', facecolor='white', alpha=0.5))

    filename = os.path.basename(file_path)
    plt.title(f"Advanced Chip Distribution (CYQ) - {filename}")
    ax1.legend(loc='upper left')
    
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    excel_file = "./stock_data/sh603667_60m_with_turnover_20260306_190920.xlsx" 
    plot_advanced_cyq(excel_file)