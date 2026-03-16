# -*- coding: utf-8 -*-
"""
===================================
XtDataFetcher - QMT/XtData Source (Priority 2)
===================================

Data from: xtquant.xtdata (Xuntou QMT)
Characteristics: Fast, requires local MiniQMT terminal running.

迅投版本比较：https://xuntou.net/#/productvip?vipType=ZDY&id=9VlLoc
注意：
1.需要安装 xtquant 包，并且本地需要运行 MiniQMT 客户端（通常是通过 QMT 软件启动的）。
2.迅投国金券商版并无港股数据
"""

import logging
import os
import re
from typing import Optional

import pandas as pd
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from .base import BaseFetcher, DataFetchError, STANDARD_COLUMNS

logger = logging.getLogger(__name__)


def _is_us_code(stock_code: str) -> bool:
    """Check if the code belongs to US market."""
    code = stock_code.strip().upper()
    return bool(re.match(r'^[A-Z]{1,5}(\.[A-Z])?$', code))


class XtDataFetcher(BaseFetcher):
    """
    QMT (xtdata) fetcher implementation.
    Requires xtquant package and local MiniQMT client.
    """
    
    name = "XtDataFetcher"
    priority = int(os.getenv("XTDATA_PRIORITY", "2"))
    
    def __init__(self):
        """Initialize XtDataFetcher and lazy-load xtquant module."""
        super().__init__()
        self._xtdata = self._get_xtdata()
        self._stock_name_cache = {}
        
    def _get_xtdata(self):
        """Lazy load xtquant to prevent application crash if missing."""
        try:
            from xtquant import xtdata
            return xtdata
        except ImportError:
            logger.warning("xtquant is not installed. Please install it to use XtDataFetcher.")
            return None

    def _format_xt_code(self, stock_code: str) -> str:
        """
        Format generic stock code to xtdata format (e.g., '600519' -> '600519.SH').
        
        Args:
            stock_code: Raw stock code.
            
        Returns:
            Formatted stock code for xtdata.
        """
        code = stock_code.strip().upper()
        
        # # Protect existing HK suffix
        # if code.endswith('.HK'):
        #     return code
            
        # Clean up existing suffixes
        for suffix in ['.SH', '.SZ', '.BJ', 'SH', 'SZ', 'BJ']:
            code = code.replace(suffix, '')
            
        # Hong Kong stocks (typically 5 digits)
        if len(code) == 5 and code.isdigit():
            return f"{code}.HK"
            
        # A-shares (typically 6 digits)
        if code.startswith(('60', '68', '51')):
            return f"{code}.SH"
        elif code.startswith(('00', '30', '15')) and len(code) == 6:
            return f"{code}.SZ"
        elif code.startswith(('4', '8')):
            return f"{code}.BJ"
        else:
            return f"{code}.SH"  # Fallback

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((ConnectionError, TimeoutError, DataFetchError)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def _fetch_raw_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Fetch historical daily data using xtdata.
        """
        if self._xtdata is None:
            raise DataFetchError("xtquant is missing. Cannot fetch data.")
            
        if _is_us_code(stock_code):
            raise DataFetchError(f"XtDataFetcher does not support US code {stock_code}.")
            
        xt_code = self._format_xt_code(stock_code)
        
        # xtdata expects YYYYMMDD format
        str_start = start_date.replace('-', '')
        str_end = end_date.replace('-', '')
        
        try:
            # Download data first (as recommended by xtquant)
            self._xtdata.download_history_data(xt_code, "1d", str_start, str_end)
            
            # Fetch data from local cache
            res = self._xtdata.get_market_data_ex(
                field_list=[], 
                stock_list=[xt_code], 
                period="1d", 
                start_time=str_start, 
                end_time=str_end, 
                count=-1, 
                dividend_type="front_ratio", 
                fill_data=False
            )
            
            if xt_code not in res or res[xt_code].empty:
                raise DataFetchError(f"XtDataFetcher returned no data for {xt_code}.")
            
            df = res[xt_code].copy()
            
            # xtdata 的 index 通常为时间戳，且内部可能已经包含 'time' 列。
            # 直接给 index 命名为 'date' 后再 reset_index，防止重命名覆盖导致的列名重复
            df.index.name = 'date'
            df = df.reset_index()
            
            # 如果存在多余的 'time' 列，将其删除
            if 'time' in df.columns:
                df = df.drop(columns=['time'])
                
            # 防御性编程：移除任何由于意外产生的重复列
            df = df.loc[:, ~df.columns.duplicated(keep='first')]
            
            return df
            
        except Exception as e:
            if isinstance(e, DataFetchError):
                raise
            raise DataFetchError(f"XtDataFetcher failed to fetch {stock_code}: {e}")

    def _normalize_data(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        """
        Normalize raw xtdata DataFrame to standard format.
        """
        df = df.copy()
        
        # Standardize date format to YYYY-MM-DD
        df['date'] = pd.to_datetime(df['date'].astype(str).str.slice(0, 8)).dt.strftime('%Y-%m-%d')
        
        # Calculate pct_chg if not exists but preClose does (xtdata sometimes provides 'preClose')
        if 'pct_chg' not in df.columns:
            if 'preClose' in df.columns and 'close' in df.columns:
                # pct_chg = (close - preClose) / preClose * 100
                df['pct_chg'] = (df['close'] - df['preClose']) / df['preClose'] * 100
            else:
                # Fallback to pandas pct_change
                df['pct_chg'] = df['close'].pct_change() * 100
                
        df['pct_chg'] = df['pct_chg'].fillna(0).round(2)
        df['code'] = stock_code
        
        # Ensure standard columns are present
        keep_cols = ['code'] + STANDARD_COLUMNS
        existing_cols = [col for col in keep_cols if col in df.columns]
        
        return df[existing_cols]

    def get_stock_name(self, stock_code: str) -> Optional[str]:
        """
        Fetch stock name via xtdata instrument details.
        """
        if self._xtdata is None:
            return None
            
        if stock_code in self._stock_name_cache:
            return self._stock_name_cache[stock_code]
            
        try:
            xt_code = self._format_xt_code(stock_code)
            detail = self._xtdata.get_instrument_detail(xt_code)
            
            if detail and 'InstrumentName' in detail:
                name = detail['InstrumentName']
                self._stock_name_cache[stock_code] = name
                return name
        except Exception as e:
            logger.debug(f"XtDataFetcher failed to get name for {stock_code}: {e}")
            
        return None

    def get_realtime_quote(self, stock_code: str) -> Optional[dict]:
        """
        Fetch realtime quote via full tick data.
        """
        if self._xtdata is None:
            return None
            
        try:
            xt_code = self._format_xt_code(stock_code)
            ticks = self._xtdata.get_full_tick([xt_code])
            
            if ticks and xt_code in ticks:
                tick = ticks[xt_code]
                return {
                    'code': stock_code,
                    'name': self.get_stock_name(stock_code) or '',
                    'price': tick.get('lastPrice', 0),
                    'open': tick.get('open', 0),
                    'high': tick.get('high', 0),
                    'low': tick.get('low', 0),
                    'pre_close': tick.get('lastClose', 0),
                    'volume': tick.get('volume', 0),
                    'amount': tick.get('amount', 0),
                    # xtdata bid/ask format is bidPrice / askPrice (arrays)
                    'bid_prices': tick.get('bidPrice', [])[:5],
                    'ask_prices': tick.get('askPrice', [])[:5],
                }
        except Exception as e:
            logger.debug(f"XtDataFetcher failed to get realtime quote for {stock_code}: {e}")
            
        return None
    
    def get_main_indices(self, region: str = "cn") -> Optional[list[dict]]:
        """
        Fetch real-time quotes for main indices (xtdata).
        Currently only supports mainland China A-shares (region='cn').
        """
        if region != "cn":
            return None
            
        if self._xtdata is None:
            return None

        # Mapping of xtdata index codes to display names and normalized full codes
        indices_map = {
            '000001.SH': ('上证指数', 'sh000001'),
            '399001.SZ': ('深证成指', 'sz399001'),
            '399006.SZ': ('创业板指', 'sz399006'),
            '000688.SH': ('科创50', 'sh000688'),
            '000016.SH': ('上证50', 'sh000016'),
            '000300.SH': ('沪深300', 'sh000300'),
        }

        try:
            xt_codes = list(indices_map.keys())
            # Fetch full tick data for all designated indices
            ticks = self._xtdata.get_full_tick(xt_codes)
            
            if not ticks:
                logger.warning("XtDataFetcher returned empty data for indices.")
                return None

            results = []
            for xt_code, (name, full_code) in indices_map.items():
                if xt_code not in ticks:
                    continue
                
                tick = ticks[xt_code]
                
                # Extract values with safe defaults
                current = tick.get('lastPrice', 0.0)
                prev_close = tick.get('lastClose', 0.0)
                high = tick.get('high', 0.0)
                low = tick.get('low', 0.0)
                open_price = tick.get('open', 0.0)
                volume = tick.get('volume', 0.0)
                amount = tick.get('amount', 0.0)
                
                # Calculate derived metrics
                change_amount = current - prev_close if prev_close else 0.0
                change_pct = (change_amount / prev_close * 100) if prev_close else 0.0
                amplitude = ((high - low) / prev_close * 100) if prev_close else 0.0

                results.append({
                    'code': full_code,
                    'name': name,
                    'current': current,
                    'change': change_amount,
                    'change_pct': change_pct,
                    'open': open_price,
                    'high': high,
                    'low': low,
                    'prev_close': prev_close,
                    'volume': volume,
                    'amount': amount,
                    'amplitude': amplitude,
                })

            if results:
                logger.info(f"XtDataFetcher successfully fetched {len(results)} indices.")
            return results if results else None

        except Exception as e:
            logger.error(f"XtDataFetcher failed to get main indices: {e}")
            return None
        
    def get_market_stats(self) -> Optional[dict]:
        """
        Fetch market rise/fall statistics via xtdata.
        Calculates up, down, flat counts and total market turnover.
        """
        if self._xtdata is None:
            return None

        try:
            # 1. Get all A-share codes. 
            # In QMT, combining "上证A股" (Shanghai A) and "深证A股" (Shenzhen A) is a safe bet.
            sh_stocks = self._xtdata.get_stock_list_in_sector("上证A股")
            sz_stocks = self._xtdata.get_stock_list_in_sector("深证A股")
            
            # Deduplicate just in case
            all_stocks = list(set((sh_stocks or []) + (sz_stocks or [])))
            
            if not all_stocks:
                logger.warning("XtDataFetcher failed to get A-share stock list for market stats.")
                return None

            # 2. Get full ticks for all stocks
            ticks = self._xtdata.get_full_tick(all_stocks)
            if not ticks:
                logger.warning("XtDataFetcher returned no ticks for market stats.")
                return None

            stats = {
                'up_count': 0,
                'down_count': 0,
                'flat_count': 0,
                'limit_up_count': 0,
                'limit_down_count': 0,
                'total_amount': 0.0,
            }

            # 3. Calculate statistics
            for code, tick in ticks.items():
                last_price = tick.get('lastPrice', 0.0)
                last_close = tick.get('lastClose', 0.0)
                amount = tick.get('amount', 0.0)
                
                stats['total_amount'] += amount

                if last_close > 0 and last_price > 0:
                    change_pct = (last_price - last_close) / last_close * 100
                    
                    if change_pct > 0:
                        stats['up_count'] += 1
                        # Note: 9.9% is a simple threshold. Actual limit up rules vary (10%, 20%, 30%) by board
                        if change_pct >= 9.9:  
                            stats['limit_up_count'] += 1
                    elif change_pct < 0:
                        stats['down_count'] += 1
                        if change_pct <= -9.9:
                            stats['limit_down_count'] += 1
                    else:
                        stats['flat_count'] += 1
                else:
                    # Treat suspended or untraded stocks as flat
                    stats['flat_count'] += 1

            # Format total amount to Yi (hundred million)
            stats['total_amount'] = stats['total_amount'] / 1e8

            logger.info(f"XtDataFetcher fetched market stats: Up={stats['up_count']}, Down={stats['down_count']}")
            return stats

        except Exception as e:
            logger.error(f"XtDataFetcher failed to get market stats: {e}")
            return None

    

if __name__ == "__main__":
    # 测试代码
    logging.basicConfig(level=logging.DEBUG)
    
    fetcher = XtDataFetcher()
    
    try:
        # 测试普通股票
        #  贵州茅台 600519.SH、平安银行 000001.SH、宁德时代 300750.SZ、有色ETF 512400.SH、沪深300 159919.SZ
        test_list = ['600519', '000001', '300750', '512400', '159919',]

        for a_share_code in test_list:
            df = fetcher.get_daily_data(a_share_code)  # 茅台
            print(f"获取成功，共 {len(df)} 条数据")
            print(df.tail())
            
            # 测试股票名称
            name = fetcher.get_stock_name(a_share_code)
            print(f"股票名称: {name}")
            
            # 测试实时行情
            quote = fetcher.get_realtime_quote(a_share_code)
            print(f"实时行情: {quote}")

        # ==========================
        # Integration test for get_main_indices
        # ==========================
        print("\n" + "=" * 50)
        print("Testing get_main_indices (xtdata)")
        print("=" * 50)
        try:
            indices = fetcher.get_main_indices()
            if indices:
                print(f"Successfully fetched {len(indices)} indices:")
                for item in indices:
                    print(f"[{item['code']}] {item['name']}: Price={item['current']:.2f}, "
                        f"Change={item['change_pct']:.2f}%, Volume={item['volume']}")
            else:
                print("Failed to fetch indices data or data is empty.")
        except Exception as e:
            print(f"Failed to fetch indices: {e}")

        # ==========================
        # Integration test for get_market_stats
        # ==========================
        print("\n" + "=" * 50)
        print("Testing get_market_stats (xtdata)")
        print("=" * 50)
        try:
            stats = fetcher.get_market_stats()
            if stats:
                print(f"Market Stats successfully computed:")
                print(f"Up: {stats['up_count']} (Limit Up: {stats['limit_up_count']})")
                print(f"Down: {stats['down_count']} (Limit Down: {stats['limit_down_count']})")
                print(f"Flat: {stats['flat_count']}")
                print(f"Total Amount: {stats['total_amount']:.2f} 亿 (Yi)")
            else:
                print("Failed to compute market stats.")
        except Exception as e:
            print(f"Failed to compute market stats: {e}")



        
    except Exception as e:
        print(f"获取失败: {e}")
