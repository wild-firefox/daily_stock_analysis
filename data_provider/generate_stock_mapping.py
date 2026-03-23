# -*- coding: utf-8 -*-
"""
生成全市场股票代码与名称的标准化映射 (使用 Tushare)
"""

import json
import logging
import tushare as ts
from tqdm import tqdm

# 引入项目内置配置与标准化函数
from src.config import get_config, setup_env
from data_provider.base import normalize_stock_code

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def build_global_stock_mapping_tushare():
    # 强制加载 .env 中的配置
    setup_env()
    config = get_config()
    
    if not config.tushare_token:
        logging.error("Tushare Token 未配置！请在 .env 中设置 TUSHARE_TOKEN。")
        return {}
        
    ts.set_token(config.tushare_token)
    pro = ts.pro_api()
    mapping = {}

    logging.info("初始化 Tushare 成功，开始拉取市场数据...")

    # 1. 获取 A 股基础信息
    try:
        logging.info("正在拉取 A股 列表...")
        df_a = pro.stock_basic(list_status='L', fields='ts_code,name')
        if df_a is not None and not df_a.empty:
            for _, row in tqdm(df_a.iterrows(), total=len(df_a), desc="A股"):
                norm_code = normalize_stock_code(str(row['ts_code']))
                mapping[norm_code] = str(row['name']).strip()
    except Exception as e:
        logging.error(f"拉取 A股 失败: {e}")

    # 2. 获取 ETF 基金信息 (场内基金)
    try:
        logging.info("正在拉取 ETF 列表...")
        df_etf = pro.fund_basic(market='E', status='L', fields='ts_code,name')
        if df_etf is not None and not df_etf.empty:
            for _, row in tqdm(df_etf.iterrows(), total=len(df_etf), desc="ETF"):
                norm_code = normalize_stock_code(str(row['ts_code']))
                mapping[norm_code] = str(row['name']).strip()
    except Exception as e:
        logging.error(f"拉取 ETF 失败: {e}")

    # 3. 获取港股基础信息 (注意：Tushare 港股接口可能需要较高积分)
    try:
        logging.info("正在拉取 港股 列表...")
        df_hk = pro.hk_basic(list_status='L', fields='ts_code,name')
        if df_hk is not None and not df_hk.empty:
            for _, row in tqdm(df_hk.iterrows(), total=len(df_hk), desc="港股"):
                norm_code = normalize_stock_code(str(row['ts_code']))
                mapping[norm_code] = str(row['name']).strip()
    except Exception as e:
        logging.warning(f"拉取 港股 失败 (可能因积分不足): {e}")

    # 4. 获取美股基础信息 (注意：Tushare 美股接口可能需要极高积分)
    try:
        logging.info("正在拉取 美股 列表...")
        df_us = pro.us_basic(fields='ts_code,name')
        if df_us is not None and not df_us.empty:
            for _, row in tqdm(df_us.iterrows(), total=len(df_us), desc="美股"):
                norm_code = normalize_stock_code(str(row['ts_code']))
                mapping[norm_code] = str(row['ts_code']).strip() # 其name为none
    except Exception as e:
        logging.warning(f"拉取 美股 失败 (可能因积分不足): {e}")

    # ==========================================
    # 补充主要指数 (基于项目 Fetcher 静态声明),防止股票和指数混合，后续应该单独设置map
    # ==========================================
    # logging.info("正在补充主要宏观指数...")
    # indices = {
    #     'sh000001': '上证指数',
    #     'sz399001': '深证成指',
    #     'sz399006': '创业板指',
    #     'sh000688': '科创50',
    #     'sh000016': '上证50',
    #     'sh000300': '沪深300',
    #     'SPX': '标普500',
    #     'IXIC': '纳斯达克',
    #     'DJI': '道琼斯',
    #     'VIX': '恐慌指数'
    # }
    
    # for raw_code, name in indices.items():
    #     mapping[normalize_stock_code(raw_code)] = name

    # 剔除空值
    mapping = {k: v for k, v in mapping.items() if k and v and k != "nan"}

    logging.info(f"成功构建映射字典，总计 {len(mapping)} 条记录。")
    return mapping

if __name__ == "__main__":
    '''
    python -m data_provider.generate_stock_mapping
    '''
    global_map = build_global_stock_mapping_tushare()
    import os
    current_dir = os.path.dirname(os.path.abspath(__file__))

    if global_map:
        output_path = os.path.join(current_dir, "stock_norm_mapping.json")
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(global_map, f, ensure_ascii=False, indent=2)
            
        logging.info(f"映射文件已保存至：{output_path}")
        
        preview_keys = list(global_map.keys())[:10]
        print("\n--- 映射预览 ---")
        for k in preview_keys:
            print(f"标准化代码: {k:<10} -> 股票名称: {global_map[k]}")