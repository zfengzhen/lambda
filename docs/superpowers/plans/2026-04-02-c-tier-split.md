# C 类细分 (C1/C2/C3/C4) 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将原来的 C 兜底层拆分为 C1/C2/C3/C4 四个子类，各有独立 OTM；同时将 OTM 模型从三元组重构为按层级映射的 dict。

**Architecture:** `classify_tier()` 在原 C fallback 处增加子分类决策树（Close vs MA20/MA60 + MA20偏离度/MACD方向）。OTM 模型从 `(otm_a, otm_b, otm_c)` 三元组改为 `dict[str, float]`，每个层级独立映射。所有消费方（backtest、tiers、latest、run.py、template）同步适配。

**Tech Stack:** Python, JavaScript (template.html 内嵌)

---

### Task 1: 扩展 classify_tier 返回 C1-C4

**Files:**
- Modify: `strategy.py:96-142`

- [ ] **Step 1: 修改 classify_tier 函数**

将末尾 `return "C"` 替换为 C1-C4 子分类决策树：

```python
def classify_tier(row: dict) -> str:
    """
    分层决策树，按优先级依次判定，首个命中即返回。

    所需字段：close, macd, prev_macd, pivot_5_pp, pivot_30_pp,
              ma20, ma60, dif, hist_vol

    层级：
      A  企稳双撑    |MACD_today| < |MACD_yesterday| AND Close > P5_PP AND Close > P30_PP
      B1 回调均线    Close < MA20 AND Close > MA60
      B2 低波整理    hist_vol < 50 AND |MA20距离| <= 5%
      B3 超跌支撑    DIF < 0 AND Close > P30_PP
      B4 趋势动能弱  MA20 > MA60 AND DIF < 0
      C1 趋势延续    Close >= MA20 AND |MA20偏离| <= 10%
      C2 过热追涨    Close >= MA20 AND |MA20偏离| > 10%
      C3 跌势减速    Close < MA60 AND |MACD| < |prev_MACD|（MACD 收窄）
      C4 加速下杀    Close < MA60 AND |MACD| >= |prev_MACD|（MACD 放大）
    """
    close = row["close"]
    macd = row["macd"]
    prev_macd = row["prev_macd"]
    p5_pp = row["pivot_5_pp"]
    p30_pp = row["pivot_30_pp"]
    ma20 = row["ma20"]
    ma60 = row["ma60"]
    dif = row["dif"]
    hist_vol = row["hist_vol"]

    # A 企稳双撑
    if abs(macd) < abs(prev_macd) and close > p5_pp and close > p30_pp:
        return "A"

    # B1 回调均线
    if close < ma20 and close > ma60:
        return "B1"

    # B2 低波整理
    ma20_dist = abs((close - ma20) / ma20 * 100)
    if hist_vol < 50 and ma20_dist <= 5:
        return "B2"

    # B3 超跌支撑
    if dif < 0 and close > p30_pp:
        return "B3"

    # B4 趋势动能弱
    if ma20 > ma60 and dif < 0:
        return "B4"

    # ── C 类细分 ──
    if close >= ma20:
        if ma20_dist > 10:
            return "C2"  # 过热追涨：价远超 MA20，超买回调风险大
        return "C1"  # 趋势延续：价在 MA20 上方但偏离合理
    if close < ma60:
        if abs(macd) < abs(prev_macd):
            return "C3"  # 跌势减速：MACD 收窄，空头力度递减
        return "C4"  # 加速下杀：MACD 放大，下行动能增强
    return "C1"  # 极少出现的 MA60 ≤ Close < MA20 边缘态
```

- [ ] **Step 2: Commit**

```
[feature/c-tier-split][功能] classify_tier 返回 C1/C2/C3/C4 子分类
```

---

### Task 2: 重构 OTM 模型为 per-tier dict

**Files:**
- Modify: `strategy.py:17-39`

- [ ] **Step 1: 替换常量定义和 get_otm_for_ticker**

删除 `DEFAULT_OTM_A / DEFAULT_OTM_B / DEFAULT_OTM_C` 三个常量及 `get_otm_for_ticker` 函数，替换为：

```python
# ---- 策略常量 ----
# 基准 OTM（3 倍杠杆标的的默认值）
DEFAULT_OTM = {
    "A": 0.10,
    "B1": 0.10, "B2": 0.10, "B3": 0.10, "B4": 0.10,
    "C1": 0.10, "C2": 0.20, "C3": 0.10, "C4": 0.20,
}

# 已知杠杆 ETF 倍数映射；不在此表中的标的默认 1 倍
LEVERAGE_MAP = {
    "TQQQ": 3, "SOXL": 3, "UPRO": 3, "SPXL": 3, "TECL": 3,
    "FNGU": 3, "BULZ": 3, "TNA": 3,
    "QLD": 2, "SSO": 2,
}

# 层级中文名
TIER_NAMES = {
    "A": "企稳双撑",
    "B1": "回调均线", "B2": "低波整理", "B3": "超跌支撑", "B4": "趋势动能弱",
    "C1": "趋势延续", "C2": "过热追涨", "C3": "跌势减速", "C4": "加速下杀",
}

ALL_TIERS = ["A", "B1", "B2", "B3", "B4", "C1", "C2", "C3", "C4"]


def get_otm_for_ticker(ticker: str) -> dict[str, float]:
    """根据标的杠杆倍数推导各层 OTM。

    基准值为 3 倍杠杆下的 DEFAULT_OTM。
    公式：floor(基准% × leverage / 3) / 100，结果为整数百分比。
    """
    leverage = LEVERAGE_MAP.get(ticker, 1)
    return {
        tier: math.floor(otm * 100 * leverage / 3) / 100
        for tier, otm in DEFAULT_OTM.items()
    }
```

- [ ] **Step 2: Commit**

```
[feature/c-tier-split][重构] OTM 模型从三元组改为 per-tier dict
```

---

### Task 3: 适配 backtest_weeks / compute_tiers / compute_latest

**Files:**
- Modify: `strategy.py:188-390`（三个函数 + _extract_rules）

- [ ] **Step 1: 修改 _extract_rules — 新增 C 子分类判定字段**

在 `_extract_rules` 返回的 dict 末尾新增 `close_vs_ma60` 字段，供前端 C 子分类面板展示：

```python
def _extract_rules(row: dict) -> dict:
    """从周数据行提取决策规则详情，供前端决策面板展示。"""
    close = row["close"]
    macd_today = row["macd"]
    macd_yesterday = row["prev_macd"]
    ma20 = row["ma20"]
    return {
        "macd_today": macd_today,
        "macd_yesterday": macd_yesterday,
        "macd_narrow": abs(macd_today) < abs(macd_yesterday),
        "p5_pp": row["pivot_5_pp"],
        "above_p5": close > row["pivot_5_pp"],
        "p30_pp": row["pivot_30_pp"],
        "above_p30": close > row["pivot_30_pp"],
        "ma20": ma20,
        "ma60": row["ma60"],
        "dif": row["dif"],
        "hist_vol": row["hist_vol"],
        "ma20_dist": round((close - ma20) / ma20 * 100, 2),
        "above_ma60": close >= row["ma60"],
    }
```

- [ ] **Step 2: 修改 backtest_weeks 签名和 OTM 查找**

将三参数 `otm_a, otm_b, otm_c` 改为单 dict 参数 `otm`：

```python
def backtest_weeks(weekly_rows: list[dict], daily_df: pd.DataFrame,
                    otm: dict[str, float] | None = None) -> list[dict]:
    """
    逐周回测：分层 → 定行权价 → 找到期日价格 → 判断是否平稳到期。

    weekly_rows: group_by_week 输出（正序）
    daily_df: 日线数据，含 date / close 列
    otm: 各层 OTM 映射，默认 DEFAULT_OTM

    返回倒序（最新一周在前）的 list[dict]。
    """
    if otm is None:
        otm = DEFAULT_OTM
    daily = daily_df.copy()
    daily["date"] = pd.to_datetime(daily["date"]).dt.date
    last_data_date = daily["date"].max()

    results = []

    for idx, row in enumerate(weekly_rows):
        tier = classify_tier(row)
        entry_date = row["date"]
        close = row["close"]
        otm_frac = otm.get(tier, 0.10)
        otm_pct = int(otm_frac * 100)
        strike = round(close * (1 - otm_frac), 2)

        # 决策规则详情
        rules = _extract_rules(row)

        # 到期日（3 周后周五）
        expiry_date = find_expiry_date(entry_date, weeks=EXPIRY_WEEKS)

        # 查到期日收盘价
        pending = False
        expiry_close = None
        pct_change = None
        period_low = None
        low_vs_strike = None
        settle_diff = None
        safe_expiry = None

        if expiry_date > last_data_date:
            pending = True
        else:
            expiry_row = daily[daily["date"] == expiry_date]
            if expiry_row.empty:
                before = daily[daily["date"] <= expiry_date].sort_values("date")
                if not before.empty:
                    expiry_close = float(before.iloc[-1]["close"])
                else:
                    pending = True
            else:
                expiry_close = float(expiry_row.iloc[0]["close"])

        if not pending and expiry_close is not None:
            pct_change = round((expiry_close - close) / close * 100, 4)
            period_rows = daily[(daily["date"] > entry_date) & (daily["date"] <= expiry_date)]
            if not period_rows.empty:
                period_low = float(period_rows["close"].min())
                low_vs_strike = round((period_low - strike) / strike * 100, 4)
            settle_diff = round((expiry_close - strike) / strike * 100, 2)
            safe_expiry = settle_diff > 0

        # 恢复天数
        recovery_days = None
        recovery_gap = None
        if safe_expiry is False:
            after = daily[daily["date"] > expiry_date].sort_values("date")
            recovered = after[after["close"] > strike]
            if not recovered.empty:
                recovery_date = recovered.iloc[0]["date"]
                recovery_days = (recovery_date - expiry_date).days
            else:
                latest_close = float(daily.iloc[-1]["close"])
                recovery_gap = round((latest_close - strike) / strike * 100, 1)

        results.append({
            "date": str(entry_date),
            "close": close,
            "tier": tier,
            "rules": rules,
            "otm": otm_pct,
            "strike": strike,
            "expiry_date": str(expiry_date),
            "expiry_close": expiry_close,
            "pct_change": pct_change,
            "period_low": period_low,
            "low_vs_strike": low_vs_strike,
            "settle_diff": settle_diff,
            "safe_expiry": safe_expiry,
            "recovery_days": recovery_days,
            "recovery_gap": recovery_gap,
            "pending": pending,
        })

    results.sort(key=lambda r: r["date"], reverse=True)
    return results
```

- [ ] **Step 3: 修改 compute_tiers**

```python
def compute_tiers(weeks: list[dict],
                   otm: dict[str, float] | None = None) -> dict:
    """
    按层级统计，包含平稳到期比例。
    返回 {tier: {name, otm, count, settled, safe_count, safe_rate}}
    """
    if otm is None:
        otm = DEFAULT_OTM

    result = {}
    for tier_key in ALL_TIERS:
        items = [w for w in weeks if w["tier"] == tier_key]
        if not items:
            continue
        settled = [w for w in items if not w["pending"]]
        safe_count = sum(1 for w in settled if w.get("safe_expiry") is True)
        safe_rate = round(safe_count / len(settled) * 100, 1) if settled else 0.0
        result[tier_key] = {
            "name": TIER_NAMES[tier_key],
            "otm": int(otm.get(tier_key, 0.10) * 100),
            "count": len(items),
            "settled": len(settled),
            "safe_count": safe_count,
            "safe_rate": safe_rate,
        }
    return result
```

- [ ] **Step 4: 修改 compute_latest**

```python
def compute_latest(weekly_rows: list[dict], daily_df: pd.DataFrame,
                    otm: dict[str, float] | None = None) -> dict:
    """
    最近一周的完整决策详情，用于前端展示。
    weekly_rows: group_by_week 输出（正序）
    daily_df: 日线数据
    """
    if not weekly_rows:
        return {}
    if otm is None:
        otm = DEFAULT_OTM

    row = weekly_rows[-1]
    tier = classify_tier(row)
    close = row["close"]
    rules = _extract_rules(row)

    # 各层行权价
    strikes = {t: round(close * (1 - o), 2) for t, o in otm.items()}

    expiry_date = find_expiry_date(row["date"], weeks=EXPIRY_WEEKS)

    otm_frac = otm.get(tier, 0.10)
    otm_pct = int(otm_frac * 100)

    return {
        "date": str(row["date"]),
        "close": close,
        "tier": tier,
        "rules": rules,
        "otm": otm_pct,
        "strikes": strikes,
        "expiry_date": str(expiry_date),
    }
```

注意：`compute_latest` 返回值变化：
- 移除 `strike_a`, `strike_b`, `strike_c` → 改为 `strikes` dict
- 新增 `otm` 字段（整数百分比）

- [ ] **Step 5: Commit**

```
[feature/c-tier-split][重构] backtest/tiers/latest 适配 per-tier OTM dict
```

---

### Task 4: 适配 run.py

**Files:**
- Modify: `run.py`

- [ ] **Step 1: 修改 run.py 的 import 和调用**

更新 import：删除 `DEFAULT_OTM_A, DEFAULT_OTM_B, DEFAULT_OTM_C`，改为 `DEFAULT_OTM, ALL_TIERS`。

修改 `compute_strategy` 函数中的调用：

```python
# 旧代码（删除）:
# otm_a, otm_b, otm_c = get_otm_for_ticker(ticker)
# logger.info(f"[{ticker}] OTM: A={otm_a*100:.0f}% B={otm_b*100:.0f}% C={otm_c*100:.0f}%")
# weeks = backtest_weeks(weekly_rows, df, otm_a=otm_a, otm_b=otm_b, otm_c=otm_c)
# tiers = compute_tiers(weeks, otm_a=otm_a, otm_b=otm_b, otm_c=otm_c)
# latest = compute_latest(weekly_rows, df, otm_a=otm_a, otm_b=otm_b, otm_c=otm_c)

# 新代码:
otm = get_otm_for_ticker(ticker)
logger.info(f"[{ticker}] OTM: {' '.join(f'{t}={int(v*100)}%' for t,v in otm.items())}")

weekly_rows = group_by_week(df)
weeks = backtest_weeks(weekly_rows, df, otm=otm)

enrich_weeks_with_options(ticker, weeks)

summary = compute_summary(weeks)
tiers = compute_tiers(weeks, otm=otm)
latest = compute_latest(weekly_rows, df, otm=otm)
```

修改 latest 的 strike 查询（适配 `strikes` dict）：

```python
# 旧代码（删除）:
# lt_strike = latest.get("strike_a") if latest["tier"] == "A" else (
#     latest.get("strike_c") if latest["tier"] == "C" else latest.get("strike_b"))

# 新代码:
lt_strike = latest.get("strikes", {}).get(latest["tier"])
```

修改 otm_config 输出：

```python
# 旧代码: "otm_config": {"otm_a": otm_a, "otm_b": otm_b, "otm_c": otm_c},
# 新代码:
"otm_config": otm,
```

- [ ] **Step 2: Commit**

```
[feature/c-tier-split][适配] run.py 使用 per-tier OTM dict
```

---

### Task 5: 更新 template.html

**Files:**
- Modify: `template.html`

- [ ] **Step 1: 更新 JS 常量 — TIER_COLORS / TIER_NAMES / activeTiers / filter**

替换 `TIER_COLORS`、`TIER_NAMES`、`activeTiers` 定义（约 190-199 行）：

```javascript
const TIER_COLORS = {
  A:  {color:'#4fc3f7', bg:'#0d1f2b'},
  B1: {color:'#ab47bc', bg:'#1a0d2b'},
  B2: {color:'#ff7043', bg:'#2b1a0d'},
  B3: {color:'#66bb6a', bg:'#0d2b1a'},
  B4: {color:'#ffa726', bg:'#2b200d'},
  C1: {color:'#90a4ae', bg:'#1a2028'},
  C2: {color:'#ef5350', bg:'#2b0d0d'},
  C3: {color:'#26c6da', bg:'#0d2b2b'},
  C4: {color:'#e53935', bg:'#2b0d0d'}
};
const TIER_NAMES = {A:'企稳双撑',B1:'回调均线',B2:'低波整理',B3:'超跌支撑',B4:'趋势动能弱',C1:'趋势延续',C2:'过热追涨',C3:'跌势减速',C4:'加速下杀'};
const activeTiers = new Set(['A','B1','B2','B3','B4','C1','C2','C3','C4']);
```

- [ ] **Step 2: 更新 renderOverview — tier 顺序**

在 `renderOverview` 函数中，将 `const order` 更新：

```javascript
const order = ['A','B1','B2','B3','B4','C1','C2','C3','C4'];
```

- [ ] **Step 3: 更新 renderFilter — tier 列表和颜色**

```javascript
function renderFilter() {
  const tiers = ['A','B1','B2','B3','B4','C1','C2','C3','C4'];
  const colors = {A:'#4fc3f7',B1:'#ab47bc',B2:'#ff7043',B3:'#66bb6a',B4:'#ffa726',C1:'#90a4ae',C2:'#ef5350',C3:'#26c6da',C4:'#e53935'};
  document.getElementById('tierFilter').innerHTML = tiers.map(t => {
    const cls = activeTiers.has(t) ? 'active' : '';
    return `<span class="filter-chip ${cls}" data-tier="${t}" style="color:${colors[t]};border-color:${colors[t]}" onclick="toggleTier('${t}')">${t}</span>`;
  }).join('');
}
```

- [ ] **Step 4: 更新 buildDecisionHtml — 决策面板新增 C 子分类规则**

在 B4 规则之后、结论之前，新增 C 子分类规则展示。找到 `${row('B4 趋势动能弱', ...)}` 之后的 `</table>` 前，插入：

```javascript
      // ── C 子分类规则 ──
      const aboveMa20 = close >= r.ma20;
      const aboveMa60 = r.above_ma60 !== undefined ? r.above_ma60 : close >= r.ma60;
      const ma20Abs = Math.abs(r.ma20_dist);
      const c_overextend = ma20Abs > 10;
      const c_narrow = r.macd_narrow;

      let cRuleHtml = '';
      // 仅当实际层级为 C1-C4 时高亮显示
      if (data.tier && data.tier.startsWith('C')) {
        if (aboveMa20) {
          cRuleHtml = row('C 价格>MA20', '#90a4ae',
            `Close ${fmt(close)} ≥ MA20 ${fmt(r.ma20)} | |偏离| ${fmt(ma20Abs,1)}% ${c_overextend ? '>' : '≤'} 10%`,
            true);
          cRuleHtml += row(c_overextend ? 'C2 过热追涨' : 'C1 趋势延续',
            c_overextend ? '#ef5350' : '#90a4ae',
            c_overextend ? '偏离过大，超买回调风险' : '偏离合理，趋势延续中',
            true);
        } else if (!aboveMa60) {
          cRuleHtml = row('C 价格<MA60', '#90a4ae',
            `Close ${fmt(close)} < MA60 ${fmt(r.ma60)}`,
            true);
          cRuleHtml += row(c_narrow ? 'C3 跌势减速' : 'C4 加速下杀',
            c_narrow ? '#26c6da' : '#e53935',
            c_narrow ? 'MACD 收窄，空头力度递减' : 'MACD 放大，下行动能增强',
            true);
        }
      }
```

然后在 `</table>` 之前加入 `${cRuleHtml}`。完整的 table 部分变为：

```javascript
      <table class="rule-table">
        ${row('A 企稳双撑', '#4fc3f7', macdDesc, r.macd_narrow)}
        ${row('', '#4fc3f7', p5Desc, r.above_p5)}
        ${row('', '#4fc3f7', p30Desc, r.above_p30)}
        ${row('B1 回调均线', '#ab47bc', b1Desc, b1_pass)}
        ${row('B2 低波整理', '#ff7043', b2Desc, b2_pass)}
        ${row('B3 超跌支撑', '#66bb6a', b3Desc, b3_pass)}
        ${row('B4 趋势动能弱', '#ffa726', b4Desc, b4_pass)}
        ${cRuleHtml}
      </table>
```

- [ ] **Step 5: 更新 buildDecisionHtml — conclusion 面板的颜色和 OTM**

将 `conclusionColor` 逻辑从二分改为按 tier 查颜色：

```javascript
    const tc = TIER_COLORS[data.tier] || TIER_COLORS['C1'];
    const conclusionColor = tc.color;
```

同时将 `conclusion-sell` / `conclusion-skip` 的 class 判断更新：

```javascript
    const conclusionClass = (data.tier === 'C2' || data.tier === 'C4') ? 'conclusion-skip' : 'conclusion-sell';
```

然后在 conclusion div 中使用 `conclusionClass`：

```javascript
      <div class="${conclusionClass}">
        <span style="color:${conclusionColor};font-size:16px;font-weight:bold">${tierLabel}</span>
        <span style="color:${conclusionColor};font-weight:bold;margin-left:12px">${data.otm}% OTM</span>
      </div>
```

- [ ] **Step 6: 更新 renderDecision — 适配 strikes dict**

`renderDecision` 目前直接调用 `buildDecisionHtml`，不需要改。但 `buildDecisionHtml` 底部的 ops-box（行权价展示）引用了 `data.strike_a / strike_b / strike_c`，需要适配新的 `data.strikes` dict。

在 `buildDecisionHtml` 末尾的 ops-box 部分，如果存在 `data.strikes`：

```javascript
      ${data.strikes ? `<div class="ops-box">
        <div class="ops-label">各层行权价参考</div>
        <table class="ops-table">
          ${Object.entries(data.strikes).map(([t, s]) => {
            const n = TIER_NAMES[t] || t;
            const tc = TIER_COLORS[t];
            const isCurrent = t === data.tier;
            const style = isCurrent ? 'font-weight:bold' : 'opacity:0.6';
            return `<tr style="${style}"><td style="color:${tc ? tc.color : '#8b95a5'}">${t} ${n}</td><td style="text-align:right">${s.toFixed(2)}</td></tr>`;
          }).join('')}
        </table>
      </div>` : ''}
```

- [ ] **Step 7: 更新 th 的 data-tip — 层级说明**

将表头中层级列的 `data-tip` 从：
```
策略分层：A 企稳双撑 / B1-B4 弱势守底 / C 兜底深虚观望
```
改为：
```
策略分层：A 企稳双撑 / B1-B4 弱势守底 / C1 趋势延续 / C2 过热追涨 / C3 跌势减速 / C4 加速下杀
```

- [ ] **Step 8: Commit**

```
[feature/c-tier-split][功能] template.html 支持 C1-C4 显示、过滤与决策面板
```

---

### Task 6: 更新测试

**Files:**
- Modify: `tests/test_strategy.py`

- [ ] **Step 1: 更新 import**

```python
from strategy import (
    group_by_week,
    classify_tier,
    compute_hist_vol,
    find_expiry_date,
    backtest_weeks,
    compute_summary,
    compute_tiers,
    compute_latest,
    get_otm_for_ticker,
    DEFAULT_OTM,
    LEVERAGE_MAP,
    ALL_TIERS,
    TIER_NAMES,
)
```

- [ ] **Step 2: 添加 C1/C2/C3/C4 分类测试**

在 `TestClassifyTier` 类中添加：

```python
    def test_tier_c1(self):
        """C1 趋势延续: Close >= MA20, |MA20偏离| <= 10%, 不满足 A/B"""
        row = self._base_row(close=105, ma20=100, ma60=90,
                             macd=-10, prev_macd=-5,  # MACD 放大 → 非 A
                             dif=1, hist_vol=60,       # DIF>0 → 非 B3/B4
                             pivot_5_pp=120, pivot_30_pp=120)  # 非 A
        assert classify_tier(row) == "C1"

    def test_tier_c2(self):
        """C2 过热追涨: Close >= MA20, |MA20偏离| > 10%"""
        row = self._base_row(close=115, ma20=100, ma60=90,
                             macd=-10, prev_macd=-5,
                             dif=1, hist_vol=60,
                             pivot_5_pp=120, pivot_30_pp=120)
        assert classify_tier(row) == "C2"

    def test_tier_c3(self):
        """C3 跌势减速: Close < MA60, MACD 收窄"""
        row = self._base_row(close=70, ma20=100, ma60=80,
                             macd=-3, prev_macd=-5,   # |3| < |5| → MACD 收窄
                             dif=-2, hist_vol=80,
                             pivot_5_pp=120, pivot_30_pp=120)  # 非 B3
        # 非 B1: close < ma60; 非 B4: ma20 > ma60 但 close < p30 使非 B3，
        # 但 B4 要 ma20>ma60 且 dif<0 → 这里 ma20=100 > ma60=80 且 dif=-2 → B4!
        # 需调整：让 ma20 < ma60 以排除 B4
        row = self._base_row(close=70, ma20=75, ma60=80,
                             macd=-3, prev_macd=-5,
                             dif=-2, hist_vol=80,
                             pivot_5_pp=120, pivot_30_pp=120)
        assert classify_tier(row) == "C3"

    def test_tier_c4(self):
        """C4 加速下杀: Close < MA60, MACD 放大"""
        row = self._base_row(close=70, ma20=75, ma60=80,
                             macd=-10, prev_macd=-5,  # |10| > |5| → MACD 放大
                             dif=-2, hist_vol=80,
                             pivot_5_pp=120, pivot_30_pp=120)
        assert classify_tier(row) == "C4"
```

- [ ] **Step 3: 更新原有 test_tier_c 测试**

原 `test_tier_c` 现在应该返回具体的 C 子类而非 "C"。更新为验证返回值是 C1-C4 之一：

```python
    def test_tier_c_returns_subtype(self):
        """原 C 兜底现在返回 C1-C4 子类"""
        row = self._base_row(close=100, dif=5, ma20=80, ma60=120,
                             macd=-10, prev_macd=-5,
                             pivot_5_pp=120, pivot_30_pp=120,
                             hist_vol=70)
        result = classify_tier(row)
        assert result.startswith("C"), f"期望 C 子类，实际 {result}"
        assert result in ("C1", "C2", "C3", "C4")
```

- [ ] **Step 4: 更新 TestBacktestWeeks**

修改 `test_backtest_tier_assigned` 的断言：

```python
    def test_backtest_tier_assigned(self, scenario):
        """每条结果都有 tier 字段"""
        weekly_rows, daily_df = scenario
        result = backtest_weeks(weekly_rows, daily_df)
        for r in result:
            assert "tier" in r
            assert r["tier"] in ("A", "B1", "B2", "B3", "B4", "C1", "C2", "C3", "C4")
```

修改 `test_backtest_otm_is_int`：

```python
    def test_backtest_otm_is_int(self, scenario):
        """otm 字段为整数"""
        weekly_rows, daily_df = scenario
        result = backtest_weeks(weekly_rows, daily_df)
        for r in result:
            assert isinstance(r["otm"], int)
            assert r["otm"] in (3, 6, 10, 20)  # 按杠杆可能的值
```

- [ ] **Step 5: 更新 TestComputeTiers**

更新 `_make_weeks` 中的 tier 值（C → C1）：

```python
    @staticmethod
    def _make_weeks():
        return [
            {"tier": "A",  "pending": False, "safe_expiry": True},
            {"tier": "A",  "pending": False, "safe_expiry": True},
            {"tier": "B1", "pending": False, "safe_expiry": False},
            {"tier": "C1", "pending": False, "safe_expiry": None},
        ]
```

更新 `test_only_traded_tiers`：
```python
    def test_only_traded_tiers(self):
        result = compute_tiers(self._make_weeks())
        assert set(result.keys()) == {"A", "B1", "C1"}
```

更新 `test_tier_otm_values`（B1 现在也是 10%）：
```python
    def test_tier_otm_values(self):
        result = compute_tiers(self._make_weeks())
        assert result["A"]["otm"] == 10
        assert result["B1"]["otm"] == 10
```

- [ ] **Step 6: 更新 TestComputeLatest**

更新 `test_required_keys`：

```python
    def test_required_keys(self):
        weekly_rows, daily_df = self._make_inputs()
        result = compute_latest(weekly_rows, daily_df)
        expected_keys = {
            "date", "close", "tier", "rules",
            "strikes", "otm", "expiry_date",
        }
        assert expected_keys.issubset(set(result.keys()))
```

更新 `test_strike_a_and_b` → `test_strikes_dict`：

```python
    def test_strikes_dict(self):
        """strikes 为 dict，包含所有层级的行权价"""
        weekly_rows, daily_df = self._make_inputs(tier_close=100.0)
        result = compute_latest(weekly_rows, daily_df)
        assert "strikes" in result
        assert result["strikes"]["A"] == pytest.approx(90.0, abs=0.01)
        assert result["strikes"]["C2"] == pytest.approx(80.0, abs=0.01)  # 20% OTM
```

更新 `test_tier_b1_assigned`：

```python
    def test_tier_b1_assigned(self):
        weekly_rows, daily_df = self._make_inputs(
            tier_close=95.0, ma20=100.0, ma60=85.0,
            macd=-10.0, prev_macd=-5.0,
        )
        result = compute_latest(weekly_rows, daily_df)
        assert result["tier"] == "B1"
        assert result["strikes"]["A"] == pytest.approx(95.0 * 0.90, abs=0.01)
        assert result["strikes"]["B1"] == pytest.approx(95.0 * 0.90, abs=0.01)
```

- [ ] **Step 7: 更新 TestGetOtmForTicker**

重写 `_expected_otm` 和测试方法，适配 dict 返回：

```python
def _expected_otm(leverage: int) -> dict[str, float]:
    """根据杠杆倍数计算期望 OTM dict"""
    return {
        tier: math.floor(otm * 100 * leverage / 3) / 100
        for tier, otm in DEFAULT_OTM.items()
    }


class TestGetOtmForTicker:
    """OTM 按杠杆倍数自动推导"""

    def test_3x_tqqq(self):
        result = get_otm_for_ticker("TQQQ")
        assert result == _expected_otm(3)
        assert result["A"] == 0.10
        assert result["C2"] == 0.20

    def test_2x_qld(self):
        result = get_otm_for_ticker("QLD")
        assert result == _expected_otm(2)
        assert result["A"] == 0.06
        assert result["C2"] == 0.13  # floor(20 * 2 / 3) = 13

    def test_1x_qqq(self):
        result = get_otm_for_ticker("QQQ")
        assert result == _expected_otm(1)
        assert result["A"] == 0.03
        assert result["C2"] == 0.06  # floor(20 * 1 / 3) = 6

    def test_unknown_ticker(self):
        assert get_otm_for_ticker("AAPL") == _expected_otm(1)

    def test_leverage_map_consistent(self):
        for ticker, lev in LEVERAGE_MAP.items():
            assert get_otm_for_ticker(ticker) == _expected_otm(lev), f"{ticker} 不匹配"
```

- [ ] **Step 8: Commit**

```
[feature/c-tier-split][测试] 更新测试适配 C1-C4 子分类和 per-tier OTM
```
