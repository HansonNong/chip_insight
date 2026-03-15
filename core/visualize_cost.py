import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from scipy.ndimage import gaussian_filter1d
from typing import Any

class ChipDistVisualizer:
    def __init__(
        self, 
        bin_count: int = 250, 
        decay_threshold: float = 0.00001, 
        smoothing: float = 1.0, 
        contrast: float = 1.0
    ) -> None:
        """Initialize parameters for market chip distribution visualization."""
        self.bin_count = bin_count
        self.decay_threshold = decay_threshold
        self.smoothing = smoothing
        self.contrast = contrast

    def _calculate_concentration(self, bins: np.ndarray, chips: np.ndarray, ratio: float = 0.9) -> str:
        """Calculate chip concentration ratio for a given percentile."""
        total = np.sum(chips)
        if total <= 0:
            return "N/A"
            
        cumsum = np.cumsum(chips) / total
        tail = (1 - ratio) / 2
        
        idx_low = np.searchsorted(cumsum, tail)
        idx_high = np.searchsorted(cumsum, 1 - tail)
        
        idx_low = max(0, min(idx_low, len(bins) - 1))
        idx_high = max(0, min(idx_high, len(bins) - 1))
        
        p_low, p_high = bins[idx_low], bins[idx_high]
        denom = p_high + p_low
        
        return f"{(p_high - p_low) / denom:.2%}" if denom != 0 else "0.00%"

    def _infer_interval(self, df: pd.DataFrame) -> str:
        """Infer K-line time interval based on datetime differences."""
        if len(df) < 2:
            return ""
            
        diffs = df['day'].diff().dt.total_seconds() / 60
        common = diffs.mode()
        
        if common.empty:
            return "Unknown"
            
        d = common.iloc[0]
        if d <= 1: return "1min"
        elif d <= 5: return "5min"
        elif d <= 65: return "60min"
        elif d <= 300: return "Daily"
        return "Weekly"

    def generate_distribution(self, df: pd.DataFrame) -> dict[str, Any]:
        """Process price data to generate chip distribution and freshness metrics."""
        if df.empty:
            return {}
            
        df['day'] = pd.to_datetime(df['day'])
        df = df.dropna(subset=['close', 'turnover_rate']).sort_values('day').reset_index(drop=True)

        # Initialize bins and trackers
        all_min, all_max = df['close'].min(), df['close'].max()
        price_bins = np.linspace(all_min * 0.95, all_max * 1.05, self.bin_count)
        
        chips = np.zeros_like(price_bins)
        freshness_chips = np.zeros_like(price_bins)
        total_steps = len(df)
        survival_weight = np.ones(len(df))
        
        # Iterative chip decay calculation
        for idx_int, (i, row) in enumerate(df.iterrows()):
            t_rate = float(row['turnover_rate'])
            chips *= (1 - t_rate)
            freshness_chips *= (1 - t_rate)

            idx = np.abs(price_bins - float(row['close'])).argmin()
            chips[idx] += t_rate
            
            current_weight = (idx_int + 1) / total_steps
            freshness_chips[idx] += (t_rate * current_weight)
            
            if idx_int < len(df) - 1:
                survival_weight[:idx_int+1] *= (1 - t_rate)

        avg_freshness = np.divide(freshness_chips, chips, out=np.zeros_like(chips), where=chips > 0)

        # Apply survival threshold and smoothing
        start_idx_arr = np.where(survival_weight > self.decay_threshold)[0]
        start_idx = start_idx_arr[0] if len(start_idx_arr) > 0 else 0
        effective_df = df.iloc[start_idx:].copy()

        if self.smoothing > 0:
            chips = gaussian_filter1d(chips, sigma=self.smoothing)
            avg_freshness = gaussian_filter1d(avg_freshness, sigma=self.smoothing)

        # Global statistics
        last_p = float(df['close'].iloc[-1])
        total_sum = np.sum(chips)
        avg_p = np.sum(price_bins * chips) / total_sum if total_sum > 0 else 0
        profit_r = (np.sum(chips[price_bins <= last_p]) / total_sum) * 100 if total_sum > 0 else 0

        return {
            "price_bins": price_bins, 
            "chips": chips, 
            "freshness": avg_freshness, 
            "last_price": last_p,
            "avg_price": avg_p, 
            "profit_ratio": profit_r,
            "start_date": effective_df['day'].iloc[0], 
            "effective_df": effective_df,
            "y_range": [effective_df['close'].min() * 0.95, effective_df['close'].max() * 1.05],
            "interval": self._infer_interval(df),
            "concentration": self._calculate_concentration(price_bins, chips)
        }

    def render_plot(
        self, 
        data: dict[str, Any], 
        own_chips: list[tuple[float, int]] = [], 
        cluster_threshold_prop: float = 0.02,  
        max_hold_chip_xspan: float = 0.4
    ) -> go.Figure:
        """Render a dual-pane Plotly figure showing K-line and chip distribution."""
        if not data:
            return go.Figure()

        main_title = (
            f"{data['interval']} K线"
            f"<br>起点: {data['start_date'].strftime('%Y-%m-%d')}"
        )

        fig = make_subplots(
            rows=1, cols=2, 
            shared_yaxes=True, 
            column_widths=[0.7, 0.3], 
            horizontal_spacing=0.01,
            subplot_titles=[main_title, ""] 
        )

        # Left pane: Price line
        fig.add_trace(
            go.Scatter(
                x=data["effective_df"]['day'], 
                y=data["effective_df"]['close'],
                name='价格', 
                line=dict(color='#2196F3', width=2), 
                showlegend=False
            ), 
            row=1, col=1
        )

        # Right pane: Chip distribution bars
        bar_colors = []
        for p, f in zip(data['price_bins'], data['freshness']):
            hue = 0 if p <= data['last_price'] else 210
            f_adj = np.power(f, self.contrast)
            sat = 25 + (f_adj * 75)
            light = 85 - (f_adj * 40)
            bar_colors.append(f"hsl({hue}, {sat}%, {light}%)")

        fig.add_trace(
            go.Bar(
                x=data["chips"], y=data["price_bins"], 
                orientation='h',
                marker=dict(color=bar_colors, line=dict(width=0)), 
                opacity=0.9, 
                showlegend=False, 
                customdata=data['freshness'],
                hovertemplate="价格: %{y:.2f}<br>筹码密度: %{x:.4f}<br>新鲜度: %{customdata:.2%}<extra></extra>",
                name="市场筹码"
            ), 
            row=1, col=2
        )

        # Overlay personal holding clusters
        if own_chips:
            y_span = data["y_range"][1] - data["y_range"][0]
            cluster_threshold = y_span * cluster_threshold_prop
            max_chip_density = max(data['chips']) if max(data['chips']) > 0 else 1
            
            own_chips.sort(key=lambda x: x[0], reverse=True)
            
            clusters: list[list[tuple[float, int]]] = []
            if own_chips:
                current_cluster = [own_chips[0]]
                for i in range(1, len(own_chips)):
                    if abs(current_cluster[-1][0] - own_chips[i][0]) < cluster_threshold:
                        current_cluster.append(own_chips[i])
                    else:
                        clusters.append(current_cluster)
                        current_cluster = [own_chips[i]]
                clusters.append(current_cluster)

            global_max_vol = max([v for p, v in own_chips])

            for cluster in clusters:
                if len(cluster) == 1:
                    price, vol = cluster[0]
                    line_length = max_chip_density * max_hold_chip_xspan * (vol / global_max_vol)
                    
                    fig.add_trace(go.Scatter(
                        x=[0, line_length], y=[price, price],
                        mode="lines", line=dict(color="#333", width=1),
                        showlegend=False, hoverinfo="skip"
                    ), row=1, col=2)

                    fig.add_annotation(
                        x=line_length, y=price,
                        text=f" {price:.2f} ({vol/100:.0f}手)",
                        showarrow=False, xanchor="left", yanchor="middle",
                        font=dict(size=10), xref="x2", yref="y2"
                    )
                else:
                    prices = [c[0] for c in cluster]
                    vols = [c[1] for c in cluster]
                    total_vol = sum(vols)
                    min_p, max_p = min(prices), max(prices)
                    avg_p = sum(p * v for p, v in zip(prices, vols)) / total_vol
                    
                    cluster_width = max_chip_density * max_hold_chip_xspan * (total_vol / global_max_vol)

                    fig.add_trace(go.Scatter(
                        x=[0, cluster_width, cluster_width, 0],
                        y=[min_p, min_p, max_p, max_p],
                        fill="toself", fillcolor="rgba(100, 100, 100, 0.2)",
                        line=dict(color="rgba(0,0,0,0.5)", width=1),
                        hovertemplate=f"聚合持仓<br>均价: {avg_p:.2f}<br>总数: {total_vol}<extra></extra>",
                        showlegend=False
                    ), row=1, col=2)

                    fig.add_annotation(
                        x=cluster_width, y=(min_p + max_p) / 2,
                        text=f" <b>{avg_p:.2f} ({total_vol/100:.0f}手)</b>",
                        showarrow=False, xanchor="left", yanchor="middle",
                        font=dict(size=10), 
                        xref="x2", yref="y2"
                    )

        # Static reference lines
        fig.add_hline(
            y=data['last_price'], 
            line_dash="dot", 
            line_color="red", 
            row=1,  # type: ignore
            col=1,  # type: ignore
            annotation_text=f"最新价:{data['last_price']:.2f}"
        )
        
        fig.add_hline(
            y=data["avg_price"], 
            line_dash="dot", 
            line_color="black", 
            row=1,  # type: ignore
            col=1,  # type: ignore
            annotation_text=f"平均成本:{data['avg_price']:.2f}"
        )

        # Summary information annotation
        info_html = (
            f"<b>盈利比例:</b> {data['profit_ratio']:.2f}%<br>"
            f"<b>90%筹码集中度:</b> {data['concentration']}"
        )
        fig.add_annotation(
            xref="paper", yref="paper", x=0.98, y=1.1, text=info_html, 
            showarrow=False, align="right", bgcolor="rgba(255, 255, 255, 0.9)", borderwidth=1
        )

        fig.update_yaxes(range=data["y_range"], row=1, col=1)
        fig.update_layout(
            template="plotly_white", 
            margin=dict(l=10, r=10, t=80, b=10),
            hovermode="y unified",
            dragmode='pan',
            xaxis=dict(fixedrange=False),
            yaxis=dict(fixedrange=True),
            modebar=dict(
                remove=['zoom2d', 'select2d', 'lasso2d', 'zoomIn2d', 'zoomOut2d', 'autoScale2d']
            ), 
            height=500, 
            autosize=True,
        )
        return fig

if __name__ == "__main__":
    test_file = "source/cache/sh603667_60m_20260310_235319.xlsx"
    import os

    if os.path.exists(test_file):
        df_in = pd.read_excel(test_file)
        viz = ChipDistVisualizer(bin_count=300, decay_threshold=1e-3, smoothing=1.5, contrast=3) 
        res = viz.generate_distribution(df_in)

        # Sample holdings for visualization
        test_own_chips = [
            (df_in['close'].iloc[-10], 500), 
            (df_in['close'].iloc[-5], 1000), 
            (df_in['close'].iloc[-1], 800)
        ]
        
        if res: 
            fig = viz.render_plot(res, own_chips=test_own_chips)
            fig.show()
    else:
        print(f"文件 {test_file} 不存在！")