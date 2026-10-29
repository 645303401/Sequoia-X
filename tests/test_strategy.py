"""策略引擎属性测试。"""

import tempfile
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from hypothesis import given
from hypothesis import settings as h_settings
from hypothesis import strategies as st

from sequoia_x.core.config import Settings
from sequoia_x.data.engine import DataEngine
from sequoia_x.strategy.base import BaseStrategy
from sequoia_x.strategy.ma_volume import MaVolumeStrategy
from sequoia_x.strategy.rps_breakout import RpsBreakoutStrategy


# Feature: sequoia-x-v2, Property 9: 策略 run() 返回值类型正确
@given(
    symbols=st.lists(
        st.text(min_size=6, max_size=6, alphabet="0123456789"),
        min_size=0,
        max_size=3,
        unique=True,
    )
)
@h_settings(max_examples=30, deadline=None)
def test_strategy_run_returns_list_of_str(symbols: list[str]) -> None:
    """属性 9：run() 应返回 list[str]，每个元素为非空字符串。"""
    with tempfile.TemporaryDirectory() as tmp_dir:
        settings = Settings(
            db_path=str(Path(tmp_dir) / "test.db"),
            start_date="2024-01-01",
            feishu_webhook_url="https://example.com/hook",
        )
        engine = DataEngine(settings)

        with patch.object(engine, "get_all_symbols", return_value=symbols):
            with patch.object(engine, "get_ohlcv_bulk", return_value=pd.DataFrame()):
                strategy = MaVolumeStrategy(engine=engine, settings=settings)
                result = strategy.run()

    assert isinstance(result, list)
    assert all(isinstance(s, str) and len(s) > 0 for s in result)


def test_limit_ratio_covers_main_growth_and_bse_boards() -> None:
    assert BaseStrategy._limit_ratio("000001") == 0.10
    assert BaseStrategy._limit_ratio("300001") == 0.20
    assert BaseStrategy._limit_ratio("688001") == 0.20
    assert BaseStrategy._limit_ratio("830799") == 0.30


def test_rps_breakout_does_not_use_today_high() -> None:
    """RPS 突破窗口只应包含此前数据，今日冲高回落不能污染基准。"""
    dates = pd.date_range("2024-01-01", periods=121, freq="D")
    frame = pd.DataFrame(
        {
            "symbol": ["000001"] * 121,
            "date": dates,
            "open": [1.0] * 120 + [100.0],
            "high": [10.0] * 120 + [200.0],
            "low": [0.9] * 120 + [90.0],
            "close": [1.0] * 120 + [100.0],
            "volume": [1000.0] * 121,
            "turnover": [10000.0] * 121,
        }
    )
    with tempfile.TemporaryDirectory() as tmp_dir:
        settings = Settings(
            db_path=str(Path(tmp_dir) / "test.db"),
            start_date="2024-01-01",
            feishu_webhook_url="https://example.com/hook",
        )
        engine = DataEngine(settings)
        with patch("sqlite3.connect"):
            with patch.object(pd, "read_sql", return_value=frame):
                result = RpsBreakoutStrategy(engine=engine, settings=settings).run()
    assert result == ["000001"]
