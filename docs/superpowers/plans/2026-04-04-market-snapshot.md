# 行情快照 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在日K图表上方添加行情快照条，展示最新收盘价、周涨跌幅、OHLC、成交量。

**Architecture:** `run.py` 在输出 JSON 中新增 `market` 字段（从 `daily_bars[-1]` 和 `latest.close` 计算），`template.html` 在日K图 `<canvas>` 上方插入行情条 HTML。

**Tech Stack:** Python (run.py), HTML/CSS/JS (template.html)

---

### Task 1: run.py — 新增 market 字段

**Files:**
- Modify: `run.py:246-258`

- [ ] **Step 1: 在 `compute_strategy` 返回前构建 market dict**

在 `run.py` 第 247 行（`return {` 之前）插入：

```python
    # 行情快照：最新交易日数据 + 周涨跌幅
    last_bar = daily_bars[-1] if daily_bars else {}
    entry_close = latest["close"] if latest else None
    if last_bar and entry_close:
        market = {
            "date": last_bar["date"],
            "close": last_bar["close"],
            "open": last_bar["open"],
            "high": last_bar["high"],
            "low": last_bar["low"],
            "volume": last_bar["volume"],
            "vol_ratio": round(last_bar["volume"] / last_bar["vol_ma20"], 2) if last_bar.get("vol_ma20") else None,
            "entry_close": entry_close,
            "change": round(last_bar["close"] - entry_close, 2),
            "change_pct": round((last_bar["close"] - entry_close) / entry_close * 100, 2),
        }
    else:
        market = None
```

- [ ] **Step 2: 将 market 加入返回 dict**

在 `return {` 块中 `"daily_bars": daily_bars,` 之后新增一行：

```python
        "market": market,
```

---

### Task 2: template.html — 行情快照 CSS

**Files:**
- Modify: `template.html:82-83`（CSS 区域，在 `.safe{...}` 行之前）

- [ ] **Step 1: 添加 market-bar CSS**

在第 83 行 `.safe{color:#4caf50}` 之前插入：

```css
/* 行情快照条 */
.market-bar{display:flex;align-items:baseline;gap:20px;padding:10px 14px;margin-bottom:6px;background:#141b2d;border:1px solid #253044;border-radius:8px;flex-wrap:wrap}
.market-bar .mk-price{font-size:22px;font-weight:bold;color:#e8ecf1}
.market-bar .mk-change{font-size:15px;font-weight:bold;margin-left:4px}
.market-bar .mk-up{color:#22c55e}.market-bar .mk-down{color:#ef4444}
.market-bar .mk-item{font-size:12px;color:#8b95a5}
.market-bar .mk-item .mk-val{color:#c8d0dc;font-weight:600;margin-left:3px}
.market-bar .mk-date{font-size:11px;color:#546e7a;margin-left:auto}
```

---

### Task 3: template.html — 行情快照 HTML 渲染

**Files:**
- Modify: `template.html:134-139`（日K chart-wrap 之前）

- [ ] **Step 1: 在 chart-panel 开头添加 market-bar 容器**

将第 134-135 行：
```html
      <div class="chart-panel">
        <div class="chart-wrap">
```

替换为：
```html
      <div class="chart-panel">
        <div class="market-bar" id="marketBar"></div>
        <div class="chart-wrap">
```

- [ ] **Step 2: 添加 renderMarketBar JS 函数**

在 `renderCharts` 函数之前（约第 344 行 `/* ===== 渲染：日K + MACD 图表 */` 之前）插入：

```javascript
/* ===== 渲染：行情快照条 ===== */
function renderMarketBar(market) {
  const el = document.getElementById('marketBar');
  if (!market || !el) { if (el) el.style.display = 'none'; return; }
  const up = market.change >= 0;
  const cls = up ? 'mk-up' : 'mk-down';
  const sign = up ? '+' : '';
  const arrow = up ? '▲' : '▼';
  const volR = market.vol_ratio != null ? market.vol_ratio.toFixed(2) + 'x' : '-';
  const fmtVol = (v) => {
    if (v >= 1e8) return (v / 1e8).toFixed(2) + '亿';
    if (v >= 1e4) return (v / 1e4).toFixed(0) + '万';
    return v.toLocaleString();
  };
  el.innerHTML = `
    <span class="mk-price">${market.close.toFixed(2)}</span>
    <span class="mk-change ${cls}">${arrow} ${sign}${market.change.toFixed(2)} (${sign}${market.change_pct.toFixed(2)}%)</span>
    <span class="mk-item">开 <span class="mk-val">${market.open.toFixed(2)}</span></span>
    <span class="mk-item">高 <span class="mk-val">${market.high.toFixed(2)}</span></span>
    <span class="mk-item">低 <span class="mk-val">${market.low.toFixed(2)}</span></span>
    <span class="mk-item">量 <span class="mk-val">${fmtVol(market.volume)}</span></span>
    <span class="mk-item">量比 <span class="mk-val ${market.vol_ratio > 1.5 ? 'mk-up' : ''}">${volR}</span></span>
    <span class="mk-date">截至 ${market.date} · 入场价 ${market.entry_close.toFixed(2)}</span>`;
}
```

- [ ] **Step 3: 在页面初始化中调用 renderMarketBar**

找到调用 `renderCharts(data.daily_bars)` 的地方，在其前面加一行：

```javascript
    renderMarketBar(data.market);
```

---

### Task 4: 测试

**Files:**
- Modify: `tests/test_run.py`（如存在）或新建

- [ ] **Step 1: 编写 market 字段单元测试**

```python
def test_market_field_in_result(tmp_path, monkeypatch):
    """compute_strategy 输出应包含 market 字段，且涨跌幅计算正确。"""
    # 使用已有的 TQQQ.json 验证结构
    import json, os
    json_path = os.path.join(os.path.dirname(__file__), "..", "output", "TQQQ.json")
    if not os.path.exists(json_path):
        pytest.skip("TQQQ.json 不存在，需先运行 run.py")
    with open(json_path) as f:
        result = json.load(f)
    m = result.get("market")
    assert m is not None, "market 字段缺失"
    # 必要字段存在
    for key in ("date", "close", "open", "high", "low", "volume", "change", "change_pct", "entry_close"):
        assert key in m, f"market 缺少 {key}"
    # 涨跌幅计算验证
    expected_change = round(m["close"] - m["entry_close"], 2)
    assert m["change"] == expected_change
    expected_pct = round((m["close"] - m["entry_close"]) / m["entry_close"] * 100, 2)
    assert m["change_pct"] == expected_pct
```

- [ ] **Step 2: 用户手动运行测试验证**

```bash
python -m pytest tests/test_run.py -v -k test_market
```

---

### Task 5: 提交

- [ ] **Step 1: 预览变更并等待用户确认后提交**

```bash
git add run.py template.html tests/
git diff --cached --stat
```
