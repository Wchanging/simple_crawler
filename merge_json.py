import json
import os


def merge_json_files(directory="weibo_results2", output_file="merged.json", incremental=True):
    """
    合并指定目录下的所有JSON文件

    Args:
        directory (str): JSON文件所在目录
        output_file (str): 输出的合并JSON文件名
        incremental (bool): 是否增量更新，如果是则保留已有数据

    Returns:
        list: 合并后的数据
    """
    # 创建目录(如果不存在)
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"创建目录: {directory}")

    # 输出文件的完整路径
    output_path = os.path.join(directory, output_file)

    # 如果是增量更新模式，尝试加载现有的合并文件
    all_data = []
    if incremental and os.path.exists(output_path):
        try:
            with open(output_path, "r", encoding="utf-8") as existing_file:
                all_data = json.load(existing_file)
                print(f"已加载现有的合并文件，包含 {len(all_data)} 条数据")
        except Exception as e:
            print(f"加载现有合并文件失败: {e}")
            all_data = []

    # 获取需要合并的所有json文件
    json_files = [f for f in os.listdir(directory) if f.endswith(".json") and f != output_file]
    print(f"找到 {len(json_files)} 个JSON文件")

    # 记录处理前的数据数量
    initial_count = len(all_data)

    # 跟踪已经处理过的文件(通过文件内容的哈希)
    processed_files = set()

    # 遍历所有json文件
    for json_file in json_files:
        file_path = os.path.join(directory, json_file)
        try:
            with open(file_path, "r", encoding="utf-8") as infile:
                # 读取json文件
                data = json.load(infile)

                # 将数据添加到列表中
                if isinstance(data, list):
                    all_data.extend(data)
                else:
                    all_data.append(data)

            print(f"成功读取: {json_file}")
        except json.JSONDecodeError as e:
            print(f"解析错误 {json_file}: {e}")
        except Exception as e:
            print(f"处理 {json_file} 时出错: {e}")

    # 使用字典进行去重 (基于publish_url字段)
    unique_data = {}
    for item in all_data:
        if 'publish_url' in item:
            unique_data[item['publish_url']] = item
        else:
            # 对于没有publish_url的项，使用其JSON字符串作为键
            item_json = json.dumps(item, sort_keys=True)
            unique_data[item_json] = item

    # 转回列表
    all_data = list(unique_data.values())

    # 将合并后的数据写入到新文件中
    print(f"合并了 {len(all_data)} 条数据 (新增 {len(all_data) - initial_count} 条)")
    with open(output_path, "w", encoding="utf-8") as outfile:
        json.dump(all_data, outfile, ensure_ascii=False, indent=2)

    print(f"合并完成，已保存到 {output_path}")
    return all_data


def extract_urls_to_file(json_path="weibo_results2/merged.json", output_file="weibo_urls.txt", append=True):
    """
    从JSON文件中提取微博URL并保存到文本文件

    Args:
        json_path (str): JSON文件路径
        output_file (str): 输出的URL文本文件名
        append (bool): 是否追加到现有文件(同时去重)
    """
    # 读取合并后的JSON文件
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
        new_urls = []
        for item in data:
            # 获取微博url
            if 'publish_url' in item and item['comment_count'] > 3:
                original_url = item['publish_url']
                new_urls.append(original_url)
    print(f"提取到 {len(new_urls)} 条微博URL")
    # 加载现有的URL(如果追加模式)
    existing_urls = set()
    if append and os.path.exists(output_file):
        try:
            with open(output_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        existing_urls.add(line)
            print(f"已加载现有URL文件，包含 {len(existing_urls)} 条URL")
        except Exception as e:
            print(f"加载现有URL文件失败: {e}")

    # 合并并去重
    all_urls = set(new_urls) | existing_urls
    sorted_urls = sorted(all_urls)  # 排序以保持一致性

    # 写入到txt文件
    with open(output_file, "w", encoding="utf-8") as f:
        for url in sorted_urls:
            f.write(url + "\n")

    print(f"已保存 {len(sorted_urls)} 条微博URL到 {output_file} (新增 {len(all_urls) - len(existing_urls)} 条)")


def main(directory="weibo_results2", merged_json="merge.json", urls_file="weibo_urls.txt"):
    """
    主函数：执行合并JSON和提取URL的完整流程
    """
    # 合并JSON文件
    # data = merge_json_files(directory, merged_json, incremental=True)

    # 提取URL并保存
    json_path = os.path.join(directory, merged_json)
    extract_urls_to_file(json_path, urls_file, append=True)


if __name__ == "__main__":
    main()
