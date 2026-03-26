# -*- coding: utf-8 -*-
"""
===================================
文本股票代码提取 (Text LLM)
===================================

从非结构化文本/对话中提取股票代码，使用通用 LLM。
"""

import logging
import random
import sys
import time
from typing import List, Optional, Tuple

from src.config import Config, get_config
from src.services.image_stock_extractor import _parse_items_from_text


logger = logging.getLogger(__name__)

class _LiteLLMPlaceholder:
    completion = None

litellm = sys.modules.get("litellm") or _LiteLLMPlaceholder()

TEXT_EXTRACT_PROMPT = """请分析以下文本/对话，提取其中提及的所有股票代码及名称。

重要：若图中同时显示股票名称和代码（如自选股列表、ETF 列表），必须同时提取两者，每个元素必须包含 code 和 name 字段。

输出格式：仅返回有效的 JSON 数组，不要 markdown、不要解释。
每个元素为对象：{"code":"股票代码","name":"股票名称","confidence":"high|medium|low"}
- code: 股票代码（A股6位、港股5位、美股1-5字母、ETF 如 159887/512880）；仅当图中确实无代码时可省略,填"XXX"
- name: 若图中有名称则必填（如 贵州茅台、银行ETF、证券ETF），与代码一一对应；仅当图中确实无名称时可省略,填"XXX"
- confidence: 必填，识别置信度，high=确定、medium=较确定、low=不确定

示例（图中同时有名称和代码时）：
- 个股：600519 贵州茅台、300750 宁德时代
- 港股：00700 腾讯控股、09988 阿里巴巴
- 美股：AAPL 苹果、TSLA 特斯拉
- ETF：159887 银行ETF、512880 证券ETF、512000 券商ETF、512480 半导体ETF、515030 新能源车ETF

输出示例：[{"code":"600519","name":"贵州茅台","confidence":"high"},{"code":"159887","name":"银行ETF","confidence":"high"}]

需要分析的文本内容如下：
{text}"""


def _get_text_models() -> List[str]:
    """获取文本模型优先级列表"""
    cfg = get_config()
    candidates = []
    
    # 优先使用配置的主用模型
    if cfg.litellm_model:
         candidates.append(cfg.litellm_model)

    if cfg.gemini_api_keys:
        candidates.extend(["gemini/gemini-2.5-flash", "gemini/gemini-2.0-flash", "gemini/gemini-3-flash-preview"])
    if cfg.anthropic_api_keys:
        candidates.append(f"anthropic/{cfg.anthropic_model or 'claude-3-5-sonnet-20241022'}")
    if cfg.openai_api_keys:
        candidates.append(f"openai/{cfg.openai_model or 'gpt-4o-mini'}")

    seen = set()
    return [x for x in candidates if not (x in seen or seen.add(x))]

def _get_api_keys_for_model(model: str, cfg: Config) -> List[str]:
    if model.startswith("gemini/") or model.startswith("vertex_ai/"):
        return [k for k in cfg.gemini_api_keys if k and len(k) >= 8]
    if model.startswith("anthropic/"):
        return [k for k in cfg.anthropic_api_keys if k and len(k) >= 8]
    return [k for k in cfg.openai_api_keys if k and len(k) >= 8]


def _enrich_and_validate_items(items: List[Tuple[Optional[str], Optional[str], str]]) -> List[Tuple[str, str, str]]:
    """使用基础信息字典进行缺位补全和严格校验"""
    # 局部引入字典防止循环导入
    from data_provider.stock_norm_mapping import STOCK_MAPPING
    # 建立名称到代码的反向字典
    NAME_TO_CODE = {v: k for k, v in STOCK_MAPPING.items() if v}

    enriched = []
    for code, name, conf in items:
        # 当只有名称时，反查字典寻找代码
        if name and code == 'XXX':
            code = NAME_TO_CODE.get(name)
            if not code:
                logger.warning(f"名称 '{name}' 在字典中找不到对应的代码，无法补全。")
                continue
        
        # 当只有代码时，正查字典寻找名称
        elif code and name == 'XXX':
            name = STOCK_MAPPING.get(code)
            if not name:
                logger.warning(f"代码 '{code}' 在字典中找不到对应的名称，无法补全。")
                continue
                
        # 当均存在时，可选检验其是否存在于字典中
        elif code and name:
            if code not in STOCK_MAPPING and name not in NAME_TO_CODE:
                 logger.warning(f"代码 '{code}' 和名称 '{name}' 在字典中均不存在，可能是识别错误。")
                 continue

        # 兜底校验
        if not code or not name:
                logger.warning(f"提取结果缺失代码或名称且无法互补填全: code='{code}', name='{name}'。请手动填写代码或名称。")
                continue

            
        enriched.append((code, name, conf))
        
    return enriched

def extract_stock_codes_from_text(text: str) -> Tuple[List[Tuple[str, Optional[str], str]], str]:
    """使用 LLM 从纯文本中提取股票代码。"""
    text = text.strip()
    if not text:
        return [], ""
        
    # 截断超长文本以节省 token (约保留前 1万字符)
    #text = text[:10000]

    models = _get_text_models()
    if not models:
        raise ValueError("未配置任何 LLM API Key，无法提取文本。")

    cfg = get_config()
    prompt = TEXT_EXTRACT_PROMPT.replace("{text}", text)
    
    global litellm
    if getattr(litellm, "completion", None) is None:
        import litellm as litellm_module
        litellm = litellm_module

    last_error: Optional[Exception] = None
    
    for model in models:
        keys = _get_api_keys_for_model(model, cfg)
        if not keys:
            continue

        for attempt in range(2):
            try:
                key = random.choice(keys) if keys else None
                kwargs = {
                    "model": model,
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 1024,
                    "api_key": key,
                    "timeout": 30,
                }
                
                if not model.startswith("gemini/") and not model.startswith("anthropic/"):
                    if cfg.openai_base_url:
                        kwargs["api_base"] = cfg.openai_base_url

                response = litellm.completion(**kwargs)
                raw = response.choices[0].message.content
                items = _parse_items_from_text(raw)

                items = _enrich_and_validate_items(items)
                
                logger.info(
                    f"[TextExtractor] {model} 提取 {len(items)} 个: "
                    f"{[(i[0], i[1]) for i in items[:5]]}{'...' if len(items) > 5 else ''}"
                )
                return items, raw
            except Exception as e:
                last_error = e
                error_msg = str(e).lower()
                
                if any(k in error_msg for k in ["429", "quota", "limit", "exhausted"]):
                    logger.warning(f"[TextExtractor] 模型 {model} 额度受限，切换备用...")
                    break 

                if attempt == 0:
                    time.sleep(1)

    raise ValueError(f"文本提取调用失败，已穷尽所有可用模型。最后错误: {last_error}") from last_error

if __name__ == "__main__":
    '''
    python -m src.services.text_stock_extractor
    '''
    from src.config import setup_env
    setup_env()
    import os
    # Proxy config - controlled by USE_PROXY env var, off by default.
    # Set USE_PROXY=true in .env if you need a local proxy (e.g. mainland China).
    # GitHub Actions always skips this regardless of USE_PROXY.
    if os.getenv("GITHUB_ACTIONS") != "true" and os.getenv("USE_PROXY", "false").lower() == "true":
        proxy_host = os.getenv("PROXY_HOST", "127.0.0.1")
        proxy_port = os.getenv("PROXY_PORT", "10809")
        proxy_url = f"http://{proxy_host}:{proxy_port}"
        os.environ["http_proxy"] = proxy_url
        os.environ["https_proxy"] = proxy_url
    # 简单测试
    test_text = """
    1、江瀚新材：公司根据市场规则对产品销售报价作出了调整，普遍涨幅在20%-40%。

    2、万兴科技：部分图示脑图Skills正式上线ClawHub。

    3、商务部发布《关于促进旅行服务出口 扩大入境消费的政策措施》。（陕西旅游、长白山）

    4、全球首个脑机接口创新产品获得医保编码。（创新医疗、三博脑科）

    5、霍尔木兹海峡梗阻氦气价格上涨40%。（华特气体、金宏气体）

    """
    items, raw = extract_stock_codes_from_text(test_text)
    print("提取结果:", items)
    print("原始 LLM 输出:", raw)