import pandas as pd
import re
from rapidocr_onnxruntime import RapidOCR

class TradeOCRProcessor:
    def __init__(self):
        # 初始化轻量化 OCR 引擎
        self.engine = RapidOCR()
        # 预编译正则，提高识别效率
        self.re_action = re.compile(r'买入|卖出')
        self.re_date_time = re.compile(r'\d{8,14}')  # 匹配 20260306144822 这种格式

    def get_raw_rows(self, img_path, y_threshold=20):
        """
        底层识别：获取原始行数据
        y_threshold: 同一行文字 Y 坐标的最大偏差（像素）
        """
        result, _ = self.engine(img_path)
        if not result:
            return []

        # 提取坐标中心点和文本
        items = []
        for line in result:
            coords, text, conf = line
            center_y = sum([p[1] for p in coords]) / 4
            center_x = sum([p[0] for p in coords]) / 4
            items.append({'x': center_x, 'y': center_y, 'text': text})

        # 按 Y 坐标排序并聚类成行
        items.sort(key=lambda x: x['y'])
        rows = []
        if items:
            current_row = [items[0]]
            for i in range(1, len(items)):
                if abs(items[i]['y'] - current_row[-1]['y']) <= y_threshold:
                    current_row.append(items[i])
                else:
                    current_row.sort(key=lambda x: x['x']) # 行内按 X 排序
                    rows.append([it['text'] for it in current_row])
                    current_row = [items[i]]
            current_row.sort(key=lambda x: x['x'])
            rows.append([it['text'] for it in current_row])
        return rows

    def parse_rows_to_df(self, rows):
        """
        逻辑层：基于状态机和特征识别自动划分记录
        """
        records = []
        buffer = {}

        for row in rows:
            row_str = "".join(row)
            
            # 1. 发现新记录起始点：包含“买入”或“卖出”
            action_match = self.re_action.search(row_str)
            if action_match:
                # 如果旧缓冲区已满（至少有名称和价格），保存它
                if 'name' in buffer and 'price' in buffer:
                    records.append(buffer)
                
                # 初始化新记录：通常这一行第一个元素是股票名称
                buffer = {
                    'name': row[0],
                    'action': action_match.group(),
                    'raw_data': [] # 暂存其他可能的数字
                }
                continue

            # 2. 填充缓冲区：寻找数字、日期等信息
            if 'name' in buffer:
                for item in row:
                    # 清洗数字：保留数字和小数点
                    clean_num = re.sub(r'[^\d.]', '', item)
                    if not clean_num: continue

                    # 判断是否为时间/日期
                    if len(clean_num) >= 8 and self.re_date_time.search(clean_num):
                        buffer['time'] = clean_num
                    # 判断是否为浮点数（价格或金额）
                    elif '.' in clean_num:
                        val = float(clean_num)
                        if 'price' not in buffer:
                            buffer['price'] = val
                        else:
                            buffer['amount'] = val
                    # 判断是否为整数（成交量）
                    elif clean_num.isdigit():
                        buffer['volume'] = int(clean_num)

        # 存入最后一条记录
        if 'name' in buffer and 'price' in buffer:
            records.append(buffer)

        # 转换为 DataFrame
        df = pd.DataFrame(records)
        
        # 后期清洗：转换时间格式
        if not df.empty and 'time' in df.columns:
            df['time'] = pd.to_datetime(df['time'], format='%Y%m%d%H%M%S', errors='coerce')
        
        return df

# --- 使用示例 ---
if __name__ == "__main__":
    # 填入你的截图路径
    img_file = 'test.jpg'
    
    processor = TradeOCRProcessor()
    
    print("正在识别图片内容...")
    raw_rows = processor.get_raw_rows(img_file)
    
    print("正在自动解析交易记录...")
    df_trades = processor.parse_rows_to_df(raw_rows)
    
    print("\n--- 识别结果 ---")
    if not df_trades.empty:
        # 只保留核心列并打印
        cols = ['name', 'action', 'price', 'volume', 'amount', 'time']
        print(df_trades[cols].to_string(index=False))
    else:
        print("未识别到有效交易记录。")