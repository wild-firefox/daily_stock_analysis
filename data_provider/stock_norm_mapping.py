import json
import os

def load_stock_mapping(mapping_file="stock_norm_mapping.json"):
    """加载股票代码映射字典"""
    # 获取项目根目录 (当前文件所在的上一级目录)
    base_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(base_dir, mapping_file)
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            mapping = json.load(f)
            if isinstance(mapping, dict):
                return mapping
            else:
                print(f"映射文件格式错误，预期为 JSON 对象但得到 {type(mapping)}")
                return {}
    except FileNotFoundError:
        print(f"映射文件未找到: {file_path}")
        return {}
    except json.JSONDecodeError as e:
        print(f"映射文件解析失败: {e}")
        return {}
    
STOCK_MAPPING = load_stock_mapping()