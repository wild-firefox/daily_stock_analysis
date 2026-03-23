# Image Extract Prompt (txt LLM)

本文档记录 `src/services/txt_stock_extractor.py` 中 `TEXT_EXTRACT_PROMPT` 的完整内容，便于 PR 审查时评估指令效果。

**当修改 TEXT_EXTRACT_PROMPT 时**：请同步更新此文件，并在 PR 描述中展示完整变更（before/after），以便审查者评估针对 code+name+confidence 提取的优化程度。

---

## 当前 Prompt（完整）

```
请分析以下文本/对话，提取其中提及的所有股票代码及名称。

重要：提取文本中明确提到的股票。如果同时有名称和代码，必须同时提取。每个元素必须包含 code 和 name 字段。

输出格式：仅返回有效的 JSON 数组，不要 markdown、不要解释。
每个元素为对象：{"code":"股票代码","name":"股票名称","confidence":"high|medium|low"}
- code: 必填，股票代码（A股6位、港股5位、美股1-5字母、ETF 如 159887/512880）
- name: 必填，股票名称。若文中仅有代码无名称可填 null
- confidence: 必填，识别置信度，high=明确提及、medium=可能提及、low=疑似代码

输出示例：[{"code":"600519","name":"贵州茅台","confidence":"high"},{"code":"159887","name":"银行ETF","confidence":"high"}]
禁止只返回代码数组如 ["159887","512880"]，必须使用对象格式。若未找到任何股票代码，返回：[]

需要分析的文本内容如下：
{text}
```