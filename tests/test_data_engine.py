"""数据引擎属性测试。"""

import sqlite3
import sys
import tempfile
from datetime import date
from pathlib import Path

import pandas as pd
from hypothesis import given
from hypothesis import settings as h_settings
from hypothesis import strategies as st

from sequoia_x.core.config import Settings
from sequoia_x.data.engine import DataEngine


def make_engine_in(tmp_dir: str) -> tuple[DataEngine, Settings]:
    """创建使用临时数据库的 DataEngine 实例。"""
    settings = Settings(
        db_path=str(Path(tmp_dir) / "test.db"),
        start_date="2024-01-01",
        feishu_webhook_url="https://example.com/hook",
    )
    engine = DataEngine(settings)
    return engine, settings


# Property 4: (symbol, date) 唯一约束防止重复写入
@given(
    symbol=st.text(min_size=6, max_size=6, alphabet="0123456789"),
    trade_date=st.dates(min_value=date(2024, 1, 1), max_value=date(2025, 12, 31)),
)
@h_settings(max_examples=50, deadline=None)
def test_unique_symbol_date_constraint(symbol: str, trade_date: date) -> None:
    """相同 (symbol, date) 插入两次，数据库中该组合记录数应保持为 1。"""
    with tempfile.TemporaryDirectory() as tmp_dir:
        engine, _ = make_engine_in(tmp_dir)
        row = {
            "symbol": symbol,
            "date": str(trade_date),
            "open": 10.0,
            "high": 11.0,
            "low": 9.0,
            "close": 10.5,
            "volume": 1000.0,
            "turnover": 10500.0,
        }
        df = pd.DataFrame([row])
        with sqlite3.connect(engine.db_path) as conn:
            df.to_sql("stock_daily", conn, if_exists="append", index=False, method="multi")
            try:
                df.to_sql("stock_daily", conn, if_exists="append", index=False, method="multi")
            except sqlite3.IntegrityError:
                pass
            count = conn.execute(
                "SELECT COUNT(*) FROM stock_daily WHERE symbol=? AND date=?",
                (symbol, str(trade_date)),
            ).fetchone()[0]
        assert count == 1


def test_to_baostock_code_supports_bse_codes() -> None:
    """北交所 4/8/920 开头代码必须映射到 baostock 的 bj 市场。"""
    assert DataEngine._to_baostock_code("430047") == "bj.430047"
    assert DataEngine._to_baostock_code("830799") == "bj.830799"
    assert DataEngine._to_baostock_code("920001") == "bj.920001"
    assert DataEngine._to_baostock_code("bj.430047") == "bj.430047"


def test_get_all_symbols_uses_named_stock_basic_fields(monkeypatch) -> None:
    """按字段名读取 baostock 结果，避免字段顺序变化导致股票被过滤。"""

    class FakeResult:
        fields = ["code", "code_name", "ipoDate", "outDate", "type", "status"]
        error_code = "0"

        def __init__(self) -> None:
            self._rows = iter(
                [
                    ["bj.430047", "中国重工", "2014-01-01", "", "1", "1"],
                    ["sz.000001", "平安银行", "1991-04-03", "", "1", "1"],
                    ["bj.430048", "已退市", "2014-01-01", "2025-01-01", "1", "0"],
                ]
            )

        def next(self) -> bool:
            try:
                self._current = next(self._rows)
            except StopIteration:
                return False
            return True

        def get_row_data(self) -> list[str]:
            return self._current

    class FakeBaostock:
        @staticmethod
        def login():
            return type("LoginResult", (), {"error_code": "0", "error_msg": ""})()

        @staticmethod
        def query_stock_basic(**_kwargs):
            return FakeResult()

        @staticmethod
        def logout():
            return None

    monkeypatch.setitem(sys.modules, "baostock", FakeBaostock)
    with tempfile.TemporaryDirectory() as tmp_dir:
        engine, _ = make_engine_in(tmp_dir)
        assert engine.get_all_symbols() == ["430047", "000001"]


def test_market_regime_classifies_rising_index() -> None:
    with tempfile.TemporaryDirectory() as tmp_dir:
        engine, _ = make_engine_in(tmp_dir)
        dates = pd.date_range("2024-01-01", periods=90, freq="D")
        closes = [100.0] * 60 + list(pd.Series(range(100, 130), dtype=float))
        with sqlite3.connect(engine.db_path) as conn:
            conn.executemany(
                "INSERT INTO market_daily(index_code, date, close) VALUES (?, ?, ?)",
                [("sh.000001", str(day.date()), close) for day, close in zip(dates, closes)],
            )
        regime = engine.get_market_regime()
        assert regime.iloc[-1]["regime"] == "上升"
        assert set(regime["regime"].dropna()) >= {"横盘", "上升"}


def test_market_regime_falls_back_to_stock_pool_proxy() -> None:
    with tempfile.TemporaryDirectory() as tmp_dir:
        engine, _ = make_engine_in(tmp_dir)
        dates = pd.date_range("2024-01-01", periods=90, freq="D")
        rows = []
        for symbol in ("000001", "600000"):
            closes = [100.0] * 60 + list(pd.Series(range(100, 130), dtype=float))
            rows.extend((symbol, str(day.date()), close) for day, close in zip(dates, closes))
        with sqlite3.connect(engine.db_path) as conn:
            conn.executemany(
                "INSERT INTO stock_daily(symbol, date, close) VALUES (?, ?, ?)",
                rows,
            )
        regime = engine.get_market_regime()
        assert regime["source"].iloc[-1] == "stock_equal_weight_proxy"
        assert regime["regime"].iloc[-1] == "上升"
