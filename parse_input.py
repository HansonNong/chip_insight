import pandas as pd
import re
import numpy as np
import cv2
from typing import Any, cast
from rapidocr_onnxruntime import RapidOCR

class TradeImageParser:
    def __init__(self, y_threshold: int = 20):
        self.engine = RapidOCR()
        self.re_action = re.compile(r'买入|卖出')
        self.re_date_time = re.compile(r'\d{8,14}')
        self.y_threshold = y_threshold

    def _get_rows(self, img_data: np.ndarray) -> list[list[str]]:
        result, _ = self.engine(img_data)
        if not result:
            return []

        items: list[dict[str, Any]] = []
        if result:
            for line in result:
                coords = cast(list[list[float]], line[0])
                text = str(line[1])
                
                y_coords = [float(p[1]) for p in coords]
                x_coords = [float(p[0]) for p in coords]
                
                center_y = sum(y_coords) / 4.0
                center_x = sum(x_coords) / 4.0
                items.append({'x': center_x, 'y': center_y, 'text': text})

        items.sort(key=lambda x: x['y'])
        rows: list[list[str]] = []
        
        if items:
            current_row = [items[0]]
            for i in range(1, len(items)):
                if abs(items[i]['y'] - current_row[-1]['y']) <= self.y_threshold:
                    current_row.append(items[i])
                else:
                    current_row.sort(key=lambda x: x['x'])
                    rows.append([it['text'] for it in current_row])
                    current_row = [items[i]]
            
            current_row.sort(key=lambda x: x['x'])
            rows.append([it['text'] for it in current_row])
            
        return rows

    def parse(self, img_input: str | bytes) -> pd.DataFrame:
        img_data: np.ndarray | None = None
        
        if isinstance(img_input, bytes):
            nparr = np.frombuffer(img_input, np.uint8)
            img_data = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        else:
            img_data = cv2.imread(str(img_input))

        if img_data is None:
            return pd.DataFrame()

        rows = self._get_rows(img_data)
        records: list[dict[str, Any]] = []
        buffer: dict[str, Any] = {}

        for row in rows:
            row_str = "".join(row)
            action_match = self.re_action.search(row_str)

            if action_match:
                if 'name' in buffer and 'price' in buffer:
                    records.append(buffer)
                
                buffer = {
                    'name': row[0],
                    'action': action_match.group()
                }
                continue

            if 'name' in buffer:
                for item in row:
                    clean_num = re.sub(r'[^\d.]', '', item)
                    if not clean_num: 
                        continue

                    if len(clean_num) >= 8 and self.re_date_time.search(clean_num):
                        buffer['time'] = clean_num
                    elif '.' in clean_num:
                        val = float(clean_num)
                        if 'price' not in buffer:
                            buffer['price'] = val
                        else:
                            buffer['amount'] = val
                    elif clean_num.isdigit():
                        buffer['volume'] = int(clean_num)

        if 'name' in buffer and 'price' in buffer:
            records.append(buffer)

        df = pd.DataFrame(records)
        if not df.empty and 'time' in df.columns:
            df['time'] = pd.to_datetime(df['time'], format='%Y%m%d%H%M%S', errors='coerce')
        
        return df

if __name__ == "__main__":
    # Test script for local execution
    test_image_path = "test_files/parse_record.jpg"
    parser = TradeImageParser()
    
    print(f"[*] Processing image: {test_image_path}")
    try:
        result_df = parser.parse(test_image_path)
        
        if not result_df.empty:
            print("[+] Successfully parsed records:")
            # Display all columns for verification
            print("-" * 80)
            print(result_df.to_string(index=False))
            print("-" * 80)
        else:
            print("[-] No records found. Check if the image path is correct or OCR failed.")
            
    except Exception as e:
        print(f"[!] An error occurred during parsing: {e}")