# Sequoia-X: 王者回归 | The King Returns

> A 股量化选股系统 V2 | A-Share Quantitative Stock Selection System V2

---

## 简介 | Introduction

Sequoia-X V2 是面向 A 股市场的量化选股系统，基于现代 Python 工程化标准从零重构。
系统以 OOP 架构、向量化计算和增量数据更新为核心设计原则，每日收盘后自动选股并推送至飞书群。

数据层使用 [baostock](http://baostock.com)（免费、无需注册、无限流）拉取历史及增量日 K 数据（后复权），
存储于本地 SQLite，彻底规避东方财富反爬问题。

---

## 两种运行模式

```bash
uv run python main.py               # 日常模式：8进程增量补数据 + 跑策略 + 飞书推送（2~3分钟）
uv run python main.py --backfill     # 回填模式：全市场历史K线一次性灌入（约12分钟）
```

---

## 内置策略 | Strategies

| 策略 | 说明 |
|---|---|
| **TurtleTrade** | 海龟突破：20日新高 + 成交额过亿 + 阳线防诱多，按涨幅排序 |
| **MaVolume** | 均线+放量突破 |
| **HighTightFlag** | 高而窄的旗形整理突破 |
| **LimitUpShakeout** | 涨停洗盘回踩确认 |
| **UptrendLimitDown** | 上升趋势中的跌停反包 |
| **RpsBreakout** | 欧奈尔 RPS 相对强度突破 |
| **PrivatePlacement** | 近期定向增发公告监控 |

### 策略审查与观察优先级

当前策略统一使用日线收盘数据。基于本地 2024-01-02 至 2026-10-28、等权统计信号出现后未来收益的结果，观察优先级如下：

1. **MaVolume**：20 日平均收益约 +2.21%，样本 19,280 个。重点看 5/20 日均线金叉、今日成交量相对前 20 日均量是否超过 1.5 倍。
2. **RpsBreakout**：20 日平均收益约 +1.31%，样本 65,230 个。重点看 120 日相对强度是否进入前 10%，以及收盘价是否接近此前 120 日高点。
3. **TurtleTrade**：20 日平均收益约 +0.39%，样本 82,644 个。重点看 20 日新高、成交额和收盘价位置，优先观察流动性高的股票。
4. **HighTightFlag**：20 日平均收益约 -0.71%，样本 3,805 个。信号稀少，暂作为辅助观察，不宜单独交易。
5. **UptrendLimitDown**：20 日平均收益接近 0%，样本 1,923 个。属于高风险逆势信号，必须结合次日止跌确认。
6. **LimitUpShakeout**：20 日平均收益约 -2.68%，样本 1,082 个。当前统计偏弱，暂不作为重点策略。
7. **PrivatePlacement**：事件监控策略，没有足够的本地价格回测样本，不纳入收益排序。

上述结果未扣手续费、滑点、涨跌停无法成交和停牌影响，也没有进行组合级回测。它用于确定复盘顺序，不代表收益保证。资料检索显示，动量与接近 52 周高点、再叠加趋势过滤，是较有重复验证的方向；项目现有 RPS、TurtleTrade 和 MaVolume 已覆盖这些核心信息，因此暂不新增重复策略。

### 多策略叠加统计

这里的“叠加”定义为：同一只股票在同一交易日同时满足组合内所有策略条件；成功定义为信号出现后第 5 个或第 20 个交易日收盘价高于信号日收盘价。统计区间仍为 2024-01-02 至 2026-10-28，未扣成本、滑点和涨跌停成交限制。

没有发现样本量达到 30 个且 20 日成功率超过 75% 的组合。少于 30 个样本的组合单独标记为“样本不足”，不作为有效性结论。

短期表面超过 75% 的组合：

- **TurtleTrade + HighTightFlag**：5 日成功率 83.3%，6 个样本；20 日成功率 66.7%。样本不足。
- **RpsBreakout + TurtleTrade + HighTightFlag**：5 日成功率 100.0%，2 个样本；20 日成功率 50.0%。样本严重不足。

按 20 日成功率排序的组合如下：

| 排名 | 组合 | 样本数 | 5 日成功率 | 20 日成功率 | 20 日平均收益 | 结论 |
|---:|---|---:|---:|---:|---:|---|
| 1 | MaVolume + UptrendLimitDown | 7 | 57.1% | 71.4% | 5.94% | 样本不足 |
| 2 | TurtleTrade + HighTightFlag | 6 | 83.3% | 66.7% | 2.80% | 样本不足 |
| 3 | RpsBreakout + TurtleTrade + HighTightFlag | 2 | 100.0% | 50.0% | 1.24% | 样本严重不足 |
| 4 | MaVolume + TurtleTrade | 5,545 | 44.8% | 45.3% | 1.22% | 当前最值得观察的稳定组合 |
| 5 | MaVolume + RpsBreakout | 727 | 51.3% | 44.7% | 2.56% | 可作为趋势确认组合 |
| 6 | RpsBreakout + HighTightFlag | 349 | 44.7% | 44.1% | 5.48% | 收益偏高但胜率一般 |
| 7 | MaVolume + RpsBreakout + TurtleTrade | 355 | 49.6% | 43.4% | 2.44% | 条件较严，暂不优先 |
| 8 | MaVolume + LimitUpShakeout | 112 | 33.9% | 42.9% | 0.12% | 暂不优先 |
| 9 | RpsBreakout + TurtleTrade | 16,424 | 44.9% | 41.7% | -0.07% | 叠加后没有改善 |
| 10 | RpsBreakout + LimitUpShakeout | 224 | 35.3% | 30.4% | -6.40% | 暂不使用 |
| 11 | RpsBreakout + UptrendLimitDown | 151 | 27.8% | 29.1% | -6.33% | 暂不使用 |
| 12 | MaVolume + RpsBreakout + LimitUpShakeout | 3 | 33.3% | 0.0% | -17.24% | 样本严重不足 |

目前适合散户重点复盘的是 **MaVolume + TurtleTrade**：一个提供均线与成交量确认，一个提供中期突破确认，样本量也远高于其他组合。它的 20 日成功率只有 45.3%，因此应理解为“优先观察和进一步回测”，不能理解为单独买入信号。

---

## 快速开始 | Quick Start

### 环境要求

- Python >= 3.10

### 1. 安装依赖

```bash
# 推荐使用 uv（快速包管理器）
uv sync

# 或者 pip
pip install .
```

### 2. 配置环境变量

```bash
cp .env.example .env
# 编辑 .env，填写飞书 Webhook URL
```

### 3. 首次回填历史数据

```bash
uv run python main.py --backfill
```

约 12 分钟完成 ~5200 只 A 股历史后复权日 K 数据回填。

### 4. 日常运行

```bash
uv run python main.py
```

建议配合 crontab 每个交易日收盘后自动执行：

```cron
15 19 * * 1-5 cd /root/Sequoia-X && .venv/bin/python main.py >> log.txt 2>&1
```

---

## 目录结构 | Project Structure

```
Sequoia-X/
├── main.py                      # 入口：argparse 分发日常/回填模式
├── pyproject.toml               # 依赖声明 + ruff/pytest 配置
├── .env.example                 # 环境变量模板
├── data/                        # SQLite 数据库（运行时生成，不入 git）
├── sequoia_x/
│   ├── core/
│   │   ├── config.py            # Pydantic-settings 配置管理
│   │   └── logger.py            # rich 结构化日志
│   ├── data/
│   │   └── engine.py            # 数据引擎（baostock 回填 + 增量同步 + SQLite）
│   ├── strategy/
│   │   ├── base.py              # 策略抽象基类
│   │   ├── turtle_trade.py      # 海龟交易策略
│   │   ├── ma_volume.py         # 均线放量策略
│   │   ├── high_tight_flag.py   # 高窄旗形策略
│   │   ├── limit_up_shakeout.py # 涨停洗盘策略
│   │   ├── uptrend_limit_down.py # 上升跌停策略
│   │   ├── rps_breakout.py      # RPS 突破策略
│   │   └── private_placement.py # 定增公告监控策略
│   └── notify/
│       └── feishu.py            # 飞书 Webhook 推送
└── tests/                       # 属性测试（hypothesis）
```

---

## 数据说明

- **数据源**：[baostock](http://baostock.com)（免费、无需注册、无限流）
- **复权方式**：后复权（hfq）— 历史价格不变，适合增量存储，避免除权导致数据错乱
- **存储**：本地 SQLite（`data/sequoia_v2.db`），可直接拷贝到其他机器使用
- **日常增量**：8 进程并行通过 baostock 拉取，2~3 分钟完成全市场更新

---

## 许可证 | License

MIT
