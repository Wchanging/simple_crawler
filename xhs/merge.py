import pandas as pd
import numpy as np
import json
import os
from datetime import datetime

def merge_content_csv_simple(file_path1, file_path2, output_file):
    """
    简化版的CSV合并函数，解决乱码问题
    """
    # 尝试不同的编码读取文件
    def read_csv_with_encoding(file_path):
        encodings = ['utf-8', 'gbk', 'gb2312', 'utf-8-sig', 'iso-8859-1']
        
        for encoding in encodings:
            try:
                print(f"尝试使用 {encoding} 编码读取 {file_path}")
                df = pd.read_csv(file_path, encoding=encoding)
                print(f"成功使用 {encoding} 编码读取文件")
                return df
            except UnicodeDecodeError:
                continue
            except Exception as e:
                print(f"使用 {encoding} 编码读取失败: {e}")
                continue
        
        raise Exception(f"无法读取文件 {file_path}，尝试了所有编码")
    
    try:
        # 读取文件
        print("读取第一个文件...")
        df1 = read_csv_with_encoding(file_path1)
        print(f"文件1包含 {len(df1)} 行数据")
        
        print("读取第二个文件...")
        df2 = read_csv_with_encoding(file_path2)
        print(f"文件2包含 {len(df2)} 行数据")
        
        # 合并并去重
        print("合并数据...")
        merged_df = pd.concat([df1, df2], ignore_index=True)
        
        print("去重...")
        if 'note_id' in merged_df.columns:
            merged_df = merged_df.drop_duplicates(subset=['note_id'], keep='first')
        else:
            print("警告: 未找到note_id列，使用所有列去重")
            merged_df = merged_df.drop_duplicates(keep='first')
        
        # 创建输出目录
        output_dir = os.path.dirname(output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # 保存文件，使用utf-8-sig编码（带BOM）
        print(f"保存到 {output_file}")
        merged_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        
        print(f"合并完成: {len(df1)} + {len(df2)} = {len(merged_df)} 行（去重后）")
        
        # 验证保存的文件
        print("验证保存的文件...")
        test_df = pd.read_csv(output_file, encoding='utf-8-sig')
        print(f"验证成功，保存的文件包含 {len(test_df)} 行数据")
        
    except Exception as e:
        print(f"合并过程中出错: {e}")
        import traceback
        traceback.print_exc()

def detect_file_encoding(file_path):
    """
    检测文件编码
    """
    try:
        import chardet
        with open(file_path, 'rb') as f:
            raw_data = f.read(10000)  # 读取前10000字节
            result = chardet.detect(raw_data)
            print(f"文件 {file_path} 检测到的编码: {result}")
            return result['encoding']
    except ImportError:
        print("未安装chardet库，使用默认编码检测方法")
        return None
    except Exception as e:
        print(f"编码检测失败: {e}")
        return None

def merge_content_csv_advanced(file_path1, file_path2, output_file):
    """
    高级版本的CSV合并函数，自动检测编码
    """
    # 检测文件编码
    encoding1 = detect_file_encoding(file_path1)
    encoding2 = detect_file_encoding(file_path2)
    
    try:
        # 读取文件
        print(f"使用编码 {encoding1 or 'utf-8'} 读取文件1")
        df1 = pd.read_csv(file_path1, encoding=encoding1 or 'utf-8')
        print(f"文件1包含 {len(df1)} 行数据")
        
        print(f"使用编码 {encoding2 or 'utf-8'} 读取文件2") 
        df2 = pd.read_csv(file_path2, encoding=encoding2 or 'utf-8')
        print(f"文件2包含 {len(df2)} 行数据")
        
        # 合并并去重
        merged_df = pd.concat([df1, df2], ignore_index=True)
        
        if 'comment_id' in merged_df.columns:
            merged_df = merged_df.drop_duplicates(subset=['comment_id'], keep='first')
        else:
            merged_df = merged_df.drop_duplicates(keep='first')
        
        # 保存为UTF-8 with BOM格式，Excel能正确识别
        merged_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        
        print(f"合并完成: {len(df1)} + {len(df2)} = {len(merged_df)} 行（去重后）")
        print(f"文件已保存为UTF-8 with BOM格式: {output_file}")
        
    except Exception as e:
        print(f"高级合并失败，回退到简单模式: {e}")
        merge_content_csv_simple(file_path1, file_path2, output_file)

if __name__ == "__main__":
    # 示例文件路径
    file_path1 = 'xhs/xhs_results_merge/comments_1.csv'
    file_path2 = 'xhs/xhs_results_merge/comments_2.csv'
    output_file = 'xhs/xhs_results_merge/xhs_merged_comments.csv'
    
    # 调用高级合并函数
    merge_content_csv_advanced(file_path1, file_path2, output_file)
    
    # 如果还有问题，可以尝试这些方法：
    print("\n=== 如果仍有乱码问题，请尝试以下方法 ===")
    print("1. 检查原始文件的编码格式")
    print("2. 用记事本打开CSV文件，另存为UTF-8格式")
    print("3. 在Excel中打开时选择'数据' -> '从文本' -> 选择UTF-8编码")