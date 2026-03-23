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

重要：提取文本中明确提到的股票。如果同时有名称和代码，必须同时提取。每个元素必须包含 code 和 name 字段。

输出格式：仅返回有效的 JSON 数组，不要 markdown、不要解释。
每个元素为对象：{"code":"股票代码","name":"股票名称","confidence":"high|medium|low"}
- code: 必填，股票代码（A股6位、港股5位、美股1-5字母、ETF 如 159887/512880）
- name: 必填，股票名称。若文中仅有代码无名称可填 null
- confidence: 必填，识别置信度，high=明确提及、medium=可能提及、low=疑似代码

输出示例：[{"code":"600519","name":"贵州茅台","confidence":"high"},{"code":"159887","name":"银行ETF","confidence":"high"}]
禁止只返回代码数组如 ["159887","512880"]，必须使用对象格式。若未找到任何股票代码，返回：[]

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