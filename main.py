"""Sequoia-X V2 主程序入口。

两种运行模式：
  python main.py               # 日常模式：8进程增量补数据 + 跑策略 + 飞书推送（2~3分钟）
  python main.py --backfill    # 回填模式：baostock 拉全市场历史K线（首次/补数据用，约12分钟）
"""

import argparse
import socket
import sys

from dotenv import load_dotenv

from sequoia_x.core.config import get_settings
from sequoia_x.core.logger import get_logger
from sequoia_x.data.engine import DataEngine
from sequoia_x.notify.feishu import FeishuNotifier
from sequoia_x.strategy.high_tight_flag import HighTightFlagStrategy
from sequoia_x.strategy.ma_volume import MaVolumeStrategy
from sequoia_x.strategy.rps_breakout import RpsBreakoutStrategy
from sequoia_x.strategy.turtle_trade import TurtleTradeStrategy

load_dotenv()
# baostock 公共服务偶尔响应较慢，给 socket 留出重试空间。
socket.setdefaulttimeout(30.0)


def _build_combined_results(strategy_results: dict[str, list[str]]) -> list[tuple[str, list[str]]]:
    """生成每日只需推送的两个策略交集结果。"""
    ma_volume = set(strategy_results.get("MaVolumeStrategy", []))
    rps = set(strategy_results.get("RpsBreakoutStrategy", []))
    turtle = set(strategy_results.get("TurtleTradeStrategy", []))
    high_tight = set(strategy_results.get("HighTightFlagStrategy", []))

    return [
        (
            "均线放量 + RPS突破 + 海龟突破",
            sorted(ma_volume & rps & turtle),
        ),
        (
            "RPS突破 + 高位旗形整理",
            sorted(rps & high_tight),
        ),
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description="Sequoia-X V2 选股系统")
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="回填模式：通过 baostock 拉取全市场历史 K 线（约12分钟）",
    )
    args = parser.parse_args()

    try:
        # 1. 初始化配置
        settings = get_settings()

        # 2. 初始化日志
        logger = get_logger(__name__)
        logger.info("Sequoia-X V2 启动")

        # 3. 初始化数据引擎
        engine = DataEngine(settings)

        if args.backfill:
            # ── 回填模式：单线程保守拉历史 K 线，自动多轮重跑 ──
            logger.info("进入回填模式...")
            all_symbols = engine.get_all_symbols()
            engine.backfill(all_symbols)
            engine.sync_market_index()
            logger.info("Sequoia-X V2 回填模式运行完成")
            return

        # ── 日常模式：单次 API 补今天 + 策略 + 推送 ──
        logger.info("开始拉取最新快照...")
        count = engine.sync_today_bulk()
        logger.info(f"快照同步完成，写入 {count} 只股票")
        market_count = engine.sync_market_index()
        logger.info(f"大盘指数同步完成，写入 {market_count} 条数据")

        # 4. 只执行用于两个组合交集的基础策略
        strategies = [
            MaVolumeStrategy(engine=engine, settings=settings),
            TurtleTradeStrategy(engine=engine, settings=settings),
            HighTightFlagStrategy(engine=engine, settings=settings),
            RpsBreakoutStrategy(engine=engine, settings=settings),
        ]

        notifier = FeishuNotifier(settings)

        # 5. 计算基础策略结果，只推送指定的两个交集组合
        strategy_results: dict[str, list[str]] = {}
        for strategy in strategies:
            strategy_name = type(strategy).__name__
            logger.info(f"执行策略：{strategy_name}")

            selected: list[str] = strategy.run()
            logger.info(f"{strategy_name} 选出 {len(selected)} 只股票")
            strategy_results[strategy_name] = selected

        combined_results = _build_combined_results(strategy_results)
        notifier.send_combined_results(combined_results, webhook_key="default")

    except Exception:
        try:
            _logger = get_logger(__name__)
            _logger.exception("主流程发生未捕获异常，程序终止")
        except Exception:
            import traceback

            traceback.print_exc()
        sys.exit(1)

    logger.info("Sequoia-X V2 运行完成")


if __name__ == "__main__":
    main()
