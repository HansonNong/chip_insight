import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from scipy.ndimage import gaussian_filter1d
from typing import Any
import os
import re

class ChipDistVisualizer:
    def __init__(self, bin_count: int = 250, decay_threshold: float = 0.00001, smoothing: float = 1.0, contrast: float = 1.0):
        self.bin_count = bin_count
        self.decay_threshold = decay_threshold
        self.smoothing = smoothing
        self.contrast = contrast

    def _calculate_concentration(self, bins: np.ndarray, chips: np.ndarray, ratio: float = 0.9) -> str:
        total = np.sum(chips)
        if total <= 0: return "N/A"
        cumsum = np.cumsum(chips) / total
        tail = (1 - ratio) / 2
        idx_low = np.searchsorted(cumsum, tail)
        idx_high = np.searchsorted(cumsum, 1 - tail)
        idx_low, idx_high = max(0, min(idx_low, len(bins)-1)), max(0, min(idx_high, len(bins)-1))
        p_low, p_high = bins[idx_low], bins[idx_high]
        denom = p_high + p_low
        return f"{(p_high - p_low) / denom:.2%}" if denom != 0 else "0.00%"

    def _infer_interval(self, df: pd.DataFrame) -> str:
        if len(df) < 2: return ""
        diffs = df['day'].diff().dt.total_seconds() / 60
        common = diffs.mode()
        if common.empty: return "Unknown"
        d = common.iloc[0]
        if d <= 1: return "1min"
        elif d <= 5: return "5min"
        elif d <= 65: return "60min"
        elif d <= 300: return "Daily"
        return "Weekly"

    def generate_distribution(self, df: pd.DataFrame) -> dict[str, Any]:
        df['day'] = pd.to_datetime(df['day'])
        df = df.dropna(subset=['close', 'turnover_rate']).sort_values('day').reset_index(drop=True)
        if df.empty: return {}

        # 1. Prepare bins based on FULL history to capture all price levels
        all_min, all_max = df['close'].min(), df['close'].max()
        price_bins = np.linspace(all_min * 0.95, all_max * 1.05, self.bin_count)
        chips = np.zeros_like(price_bins)
        
        freshness_chips = np.zeros_like(price_bins)
        total_steps = len(df)

        # Track chip survival for visual cropping
        survival_weight = np.ones(len(df))
        
        # 2. FULL calculation: Process every row to accumulate chips correctly
        for idx_int, (i, row) in enumerate(df.iterrows()):
            t_rate = float(row['turnover_rate'])
            chips *= (1 - t_rate)
            freshness_chips *= (1 - t_rate)

            idx = np.abs(price_bins - float(row['close'])).argmin()
            chips[idx] += t_rate
            
            current_weight = (idx_int + 1) / total_steps
            freshness_chips[idx] += (t_rate * current_weight)
            
            # Record how much of THIS specific day's chip remains at the end
            # This helps us find the "Effective Start" date for the chart
            if idx_int < len(df) - 1:
                survival_weight[:idx_int+1] *= (1 - t_rate)

        avg_freshness = np.divide(freshness_chips, chips, out=np.zeros_like(chips), where=chips > 0)

        # 3. Find effective start based on decay_threshold
        # We look for the first index where the historical chips still have influence
        start_idx = np.where(survival_weight > self.decay_threshold)[0]
        start_idx = start_idx[0] if len(start_idx) > 0 else 0
        effective_df = df.iloc[start_idx:].copy()

        # 4. Apply smoothing to make it look like professional software (THS)
        if self.smoothing > 0:
            chips = gaussian_filter1d(chips, sigma=self.smoothing)
            avg_freshness = gaussian_filter1d(avg_freshness, sigma=self.smoothing)

        last_p = float(df['close'].iloc[-1])
        total_sum = np.sum(chips)
        avg_p = np.sum(price_bins * chips) / total_sum if total_sum > 0 else 0
        profit_r = (np.sum(chips[price_bins <= last_p]) / total_sum) * 100 if total_sum > 0 else 0

        return {
            "price_bins": price_bins, "chips": chips, "freshness": avg_freshness, "last_price": last_p,
            "avg_price": avg_p, "profit_ratio": profit_r,
            "start_date": effective_df['day'].iloc[0], 
            "effective_df": effective_df,
            "y_range": [effective_df['close'].min() * 0.95, effective_df['close'].max() * 1.05],
            "interval": self._infer_interval(df),
            "concentration": self._calculate_concentration(price_bins, chips)
        }

    def render_plot(self, data: dict[str, Any], filename: str = "") -> go.Figure:
        if not data: return go.Figure()
        code_match = re.search(r'[a-zA-Z]{2}\d{6}', filename)
        stock_code = code_match.group(0).upper() if code_match else "Unknown"
        main_title = f"{stock_code} ({data['interval']}) - Effective Start: {data['start_date']}"

        fig = make_subplots(
            rows=1, cols=2, shared_yaxes=True, 
            column_widths=[0.7, 0.3], horizontal_spacing=0.01,
            subplot_titles=(main_title, "Profit/Loss Distribution")
        )

        # Left: Price line (Cropped to effective range)
        fig.add_trace(go.Scatter(x=data["effective_df"]['day'], y=data["effective_df"]['close'],
                                 name='Price', line=dict(color='#2196F3', width=2)), row=1, col=1)

        # Right: Chips (Full accumulation)
        bar_colors = []
        for p, f in zip(data['price_bins'], data['freshness']):
            hue = 0 if p <= data['last_price'] else 210
            
            f_adj = np.power(f, self.contrast)
            
            sat = 25 + (f_adj * 75)
            light = 85 - (f_adj * 40)
            bar_colors.append(f"hsl({hue}, {sat}%, {light}%)")

        fig.add_trace(go.Bar(x=data["chips"], y=data["price_bins"], orientation='h',
                             marker=dict(color=bar_colors, line=dict(width=0)), 
                             opacity=0.9, showlegend=False, customdata=data['freshness'],
                             hovertemplate="Price: %{y:.2f}<br>Density: %{x:.4f}<br>Freshness: %{customdata:.2%}<extra></extra>"), 
                      row=1, col=2)

        # Reference lines
        # pyright: ignore [reportGeneralTypeIssues]
        fig.add_hline(y=data['last_price'], line_dash="solid", line_color="black", row=1, col=1)
        fig.add_hline(y=data["avg_price"], line_dash="dot", line_color="blue", 
                      annotation_text=f"Avg:{data['avg_price']:.2f}", row=1, col=1)

        info_html = (f"<b>Profit Ratio:</b> {data['profit_ratio']:.2f}%<br>"
                      f"<b>90% Concentration:</b> {data['concentration']}")
        
        fig.add_annotation(xref="paper", yref="paper", x=0.98, y=1.1, text=info_html, 
                            showarrow=False, align="right", bgcolor="rgba(255, 255, 255, 0.9)", borderwidth=1)

        fig.update_yaxes(range=data["y_range"], row=1, col=1)
        fig.update_layout(template="plotly_white", margin=dict(l=50, r=50, t=110, b=50), hovermode="y unified")
        return fig

if __name__ == "__main__":
    test_file = "./cache/sh603667_60m_with_turnover_20260310_144759.xlsx"

    if os.path.exists(test_file):
        df_in = pd.read_excel(test_file)
        viz = ChipDistVisualizer(bin_count=300, decay_threshold=1e-3, smoothing=1.5, contrast=3) 
        res = viz.generate_distribution(df_in)

        if res: viz.render_plot(res, filename=os.path.basename(test_file)).show()

    else:
        print(f"File {test_file} not exist! ")