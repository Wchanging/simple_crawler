import csv
import pandas as pd
import os
import json


if __name__ == "__main__":
    # 读取CSV文件
    csv_file = "weibo_details/review_data.csv"
    df = pd.read_csv(csv_file, encoding="utf-8-sig")

    # 输出条目数
    print(f"CSV文件包含 {len(df)} 条记录")
