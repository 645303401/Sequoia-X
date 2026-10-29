"""均线+成交量选股策略：5日均线上穿20日均线且成交量放大。"""

from sequoia_x.core.logger import get_logger
from sequoia_x.strategy.base import BaseStrategy

logger = get_logger(__name__)


class MaVolumeStrategy(BaseStrategy):
    """均线+成交量选股策略。

    选股条件（全部向量化，严禁 iterrows）：
    1. 5日收盘均线上穿20日收盘均线（金叉）
    2. 当日成交量 > 20日均量的 1.5 倍（放量确认）

    Attributes:
        webhook_key: 路由到 'ma_volume' 专属飞书机器人。
    """

    webhook_key: str = "ma_volume"
    _MIN_BARS: int = 21

    def run(self) -> list[str]:
        """
        遍历全市场，返回满足均线金叉+放量条件的股票代码列表。

        Returns:
            满足条件的股票代码列表。
        """
        symbols = self.engine.get_local_symbols()
        selected: list[str] = []

        market_data = self.engine.get_ohlcv_bulk(symbols)
        if market_data.empty or "symbol" not in market_data:
            return selected
        for symbol, df in market_data.groupby("symbol", sort=False):
            try:
                if len(df) < self._MIN_BARS:
                    continue

                # 向量化计算均线和成交量均值
                df["ma5"] = df["close"].rolling(5).mean()
                df["ma20"] = df["close"].rolling(20).mean()
                # 只用前 20 个交易日作为比较基准，避免把今日放量混入均值。
                df["vol_ma20"] = df["volume"].shift(1).rolling(20).mean()

                # 取最后两行判断金叉（昨日 ma5 < ma20，今日 ma5 > ma20）
                last = df.iloc[-1]
                prev = df.iloc[-2]

                golden_cross = prev["ma5"] < prev["ma20"] and last["ma5"] > last["ma20"]
                volume_surge = last["volume"] > last["vol_ma20"] * 1.5

                if golden_cross and volume_surge:
                    selected.append(symbol)

            except Exception as exc:
                logger.warning(f"[{symbol}] 策略计算失败：{exc}")
                continue

        logger.info(f"MaVolumeStrategy 选出 {len(selected)} 只股票")
        return selected
