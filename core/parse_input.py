import pandas as pd
import re
import numpy as np
import cv2
from typing import Any, cast
from rapidocr_onnxruntime import RapidOCR
from datetime import date

class TradeImageParser:
    def __init__(self, y_threshold: int = 45):
        self.engine = RapidOCR()
        self.re_action = re.compile(r'买入|卖出')
        self.re_date_time = re.compile(r'\d{8,14}')
        self.re_time_only = re.compile(r'\d{6}') 
        self.y_threshold = y_threshold

    def _get_rows(self, img_data: np.ndarray) -> list[list[dict[str, Any]]]:
        result, _ = self.engine(img_data)
        if not result:
            return []

        items = []
        for line in result:
            coords = cast(list[list[float]], line[0])
            text = str(line[1]).strip()
            y_coords = [float(p[1]) for p in coords]
            x_coords = [float(p[0]) for p in coords]
            items.append({
                'x': sum(x_coords)/4.0, 
                'y': sum(y_coords)/4.0, 
                'text': text
            })
        
        if not items:
            return []

        # Group items into rows based on Y coordinate threshold
        items.sort(key=lambda x: x['y'])
        rows_data = []
        current_row = [items[0]]

        for i in range(1, len(items)):
            if abs(items[i]['y'] - current_row[-1]['y']) <= self.y_threshold:
                current_row.append(items[i])
            else:
                current_row.sort(key=lambda x: x['x'])
                rows_data.append(current_row)
                current_row = [items[i]]

        current_row.sort(key=lambda x: x['x'])
        rows_data.append(current_row)
        return rows_data

    def parse(self, img_input: str | bytes) -> pd.DataFrame:
        img_data = None
        if isinstance(img_input, bytes):
            nparr = np.frombuffer(img_input, np.uint8)
            img_data = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        else:
            img_data = cv2.imread(str(img_input))

        if img_data is None:
            return pd.DataFrame()

        img_width = img_data.shape[1]
        rows_data = self._get_rows(img_data)
        records = []
        buffer = {}

        for row in rows_data:
            row_texts = [it['text'] for it in row]
            row_str = "".join(row_texts)
            action_match = self.re_action.search(row_str)

            if action_match:
                # Save previous record if valid
                if 'name' in buffer and (buffer.get('price', 0) > 0 or buffer.get('volume', 0) > 0):
                    records.append(buffer.copy())
                
                # Identify stock name: Non-numeric text in the left half, length >= 2
                stock_name = "Unknown"
                for it in row:
                    x_ratio = it['x'] / img_width
                    if x_ratio < 0.5:
                        clean_num = re.sub(r'[^\d.]', '', it['text'])
                        if not clean_num and len(it['text']) >= 2:
                            stock_name = it['text']
                            break
                
                buffer = {
                    'name': stock_name, 
                    'action': action_match.group(), 
                    'price': 0.0, 
                    'volume': 0, 
                    'amount': 0.0, 
                    'time': None
                }
                search_items = row
            else:
                search_items = row

            if 'name' in buffer:
                for it in search_items:
                    item_text = it['text']
                    clean_num = re.sub(r'[^\d.]', '', item_text)
                    if not clean_num:
                        continue
                    
                    if self.re_date_time.fullmatch(clean_num):
                        buffer['time'] = clean_num
                    elif '.' in clean_num:
                        val = float(clean_num)
                        if buffer.get('price') == 0.0:
                            buffer['price'] = val
                        else:
                            buffer['amount'] = val
                    elif clean_num.isdigit():
                        val_int = int(clean_num)
                        # Avoid matching 6-digit time as volume
                        if buffer.get('volume') == 0 and not self.re_time_only.fullmatch(clean_num):
                            buffer['volume'] = val_int
                        elif self.re_time_only.fullmatch(clean_num) and buffer.get('time') is None:
                            today = date.today().strftime("%Y%m%d")
                            buffer['time'] = today + clean_num

        # Push the last record
        if 'name' in buffer and (buffer.get('price', 0) > 0 or buffer.get('volume', 0) > 0):
            records.append(buffer)

        # Post-processing with Pandas
        df = pd.DataFrame(records)
        if not df.empty:
            if 'time' in df.columns:
                df['time'] = pd.to_datetime(df['time'], format='%Y%m%d%H%M%S', errors='coerce')
                df['time'] = df['time'].ffill().fillna(pd.Timestamp.now())
            
            df['price'] = df['price'].fillna(0.0)
            df['volume'] = df['volume'].fillna(0).astype(int)
            df['amount'] = df['amount'].fillna(0.0)
            
            # Calibration: handle OCR missed amount
            mask = (df['amount'] == 0.0) & (df['price'] > 0) & (df['volume'] > 0)
            df.loc[mask, 'amount'] = df['price'] * df['volume']
            
        return df

if __name__ == "__main__":
    test_image_path = "test_files/screenshot2.jpg"
    parser = TradeImageParser()
    
    try:
        result_df = parser.parse(test_image_path)
        if not result_df.empty:
            print(result_df.to_string(index=False))
        else:
            print("No records found.")
    except Exception as e:
        print(f"Error: {e}")