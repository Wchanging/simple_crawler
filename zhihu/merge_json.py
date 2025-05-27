import json
import os


def merge_json_files(directory, output_file="zhihu/zhihu_urls.txt", incremental=True):
    # 读取指定目录下的JSON文件
    json_files = [f for f in os.listdir(directory) if f.endswith('.json')]
    print(f"找到 {len(json_files)} 个JSON文件")
    all_data = []
    # 读取json文件内容——json里面是一个列表，列表中是字典
    # 从字典中提取url
    for json_file in json_files:
        file_path = os.path.join(directory, json_file)
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    urls = [item['url'] for item in data if 'url' in item]
                    all_data.extend(urls)
                else:
                    print(f"文件 {json_file} 格式不正确，跳过")
        except Exception as e:
            print(f"读取文件 {json_file} 时出错: {e}")
    # 去重
    unique_data = list(set(all_data))
    print(f"合并后的数据量: {len(unique_data)}")
    
    # 写入到输出文件
    # 先读取现有的输出文件（如果存在）
    if incremental and os.path.exists(output_file):
        try:
            with open(output_file, 'r', encoding='utf-8') as existing_file:
                existing_data = existing_file.read().splitlines()
                existing_set = set(existing_data)
                unique_data = list(set(unique_data) - existing_set)
                print(f"现有输出文件中已有 {len(existing_set)} 条数据")
                print(f"将新增 {len(unique_data)} 条数据写入输出文件")
        except Exception as e:
            print(f"读取现有输出文件时出错: {e}")
            existing_set = set()

    # 增量更新：将新数据追加到现有文件
    try:
        with open(output_file, 'a', encoding='utf-8') as out_file:
            for url in unique_data:
                if url not in existing_set:  # 确保不重复写入
                    out_file.write(url + '\n')
        print(f"数据已写入 {output_file}")
    except Exception as e:
        print(f"写入输出文件时出错: {e}")

if __name__ == "__main__":
    # 设置要合并的目录
    directory = "zhihu/zhihu_results"
    # 设置输出文件名
    output_file = "zhihu/zhihu_urls.txt"
    # 调用合并函数
    merge_json_files(directory, output_file, incremental=True)