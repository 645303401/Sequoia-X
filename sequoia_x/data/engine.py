"""数据引擎模块：负责 SQLite 行情数据存储与 baostock 增量同步。"""

import sqlite3
import time
from pathlib import Path

import pandas as pd

from sequoia_x.core.config import Settings
from sequoia_x.core.logger import get_logger

logger = get_logger(__name__)

DEFAULT_MARKET_INDEXES = {
    "sh.000001": "上证指数",
}


_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_daily (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol   TEXT    NOT NULL,
    date     TEXT    NOT NULL,
    open     REAL,
    high     REAL,
    low      REAL,
    close    REAL,
    volume   REAL,
    turnover REAL,
    UNIQUE (symbol, date)
);
"""

_CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_symbol_date ON stock_daily (symbol, date);
"""

_CREATE_STOCK_BASIC_SQL = """
CREATE TABLE IF NOT EXISTS stock_basic (
    symbol TEXT PRIMARY KEY,
    name   TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""

_CREATE_MARKET_DAILY_SQL = """
CREATE TABLE IF NOT EXISTS market_daily (
    index_code TEXT NOT NULL,
    date       TEXT NOT NULL,
    open       REAL,
    high       REAL,
    low        REAL,
    close      REAL,
    volume     REAL,
    turnover   REAL,
    UNIQUE (index_code, date)
);
"""

_CREATE_MARKET_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_market_index_date ON market_daily (index_code, date);
"""

_UPSERT_SQL = """
INSERT INTO stock_daily (
    symbol, date, open, high, low, close, volume, turnover
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(symbol, date) DO UPDATE SET
    open = excluded.open,
    high = excluded.high,
    low = excluded.low,
    close = excluded.close,
    volume = excluded.volume,
    turnover = excluded.turnover
"""

_MARKET_UPSERT_SQL = """
INSERT INTO market_daily (
    index_code, date, open, high, low, close, volume, turnover
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(index_code, date) DO UPDATE SET
    open = excluded.open,
    high = excluded.high,
    low = excluded.low,
    close = excluded.close,
    volume = excluded.volume,
    turnover = excluded.turnover
"""


def _login_baostock(bs) -> bool:
    result = bs.login()
    if result.error_code != "0":
        logger.warning(f"baostock 登录失败：{result.error_msg}")
        return False
    return True


def _query_history_with_retry(
    bs,
    bs_code: str,
    start: str,
    end: str,
    adjustflag: str = "1",
    max_retries: int = 3,
) -> list[list[str]]:
    """查询日线并在网络异常时重连、退避重试。"""
    for attempt in range(max_retries):
        try:
            rs = bs.query_history_k_data_plus(
                bs_code,
                "date,open,high,low,close,volume,amount",
                start_date=start,
                end_date=end,
                frequency="d",
                adjustflag=adjustflag,
            )
            if rs is None or rs.error_code != "0":
                message = getattr(rs, "error_msg", "查询返回为空")
                raise RuntimeError(message)

            rows = []
            while rs.next():
                rows.append(rs.get_row_data())
            return rows
        except Exception as exc:
            if attempt >= max_retries - 1:
                logger.warning(f"[{bs_code}] {max_retries} 次请求失败：{exc}")
                return []
            wait = min(2 ** (attempt + 1), 8)
            logger.warning(f"[{bs_code}] 第 {attempt + 1} 次请求失败：{exc}，{wait}s 后重试")
            try:
                bs.logout()
            except Exception:
                pass
            time.sleep(wait)
            if not _login_baostock(bs):
                time.sleep(wait)
    return []


def _bs_fetch_batch(tasks: list) -> list:
    """多进程 worker：独立 login，批量拉取 baostock 数据。"""
    import baostock as bs

    if not _login_baostock(bs):
        return []
    results = []
    try:
        for symbol, bs_code, start, end in tasks:
            rows = _query_history_with_retry(bs, bs_code, start, end)
            results.extend([symbol] + row for row in rows)
    finally:
        bs.logout()
    return results


class DataEngine:
    """行情数据引擎，负责 SQLite 存储和 baostock 数据同步。"""

    def __init__(self, settings: Settings) -> None:
        self.db_path: str = settings.db_path
        self.start_date: str = settings.start_date
        self._init_db()

    def _init_db(self) -> None:
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(_CREATE_TABLE_SQL)
            conn.execute(_CREATE_INDEX_SQL)
            conn.execute(_CREATE_STOCK_BASIC_SQL)
            conn.execute(_CREATE_MARKET_DAILY_SQL)
            conn.execute(_CREATE_MARKET_INDEX_SQL)
            conn.commit()
        logger.info(f"数据库初始化完成：{self.db_path}")

    def _get_last_date(self, symbol: str) -> str | None:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT MAX(date) FROM stock_daily WHERE symbol = ?",
                (symbol,),
            ).fetchone()
        return row[0] if row and row[0] else None

    def _get_last_dates(self) -> dict[str, str]:
        """一次性读取所有股票的最新日期，避免回填时产生 N+1 次查询。"""
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT symbol, MAX(date) FROM stock_daily GROUP BY symbol"
            ).fetchall()
        return {symbol: last_date for symbol, last_date in rows if last_date}

    def get_ohlcv(self, symbol: str) -> pd.DataFrame:
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql(
                """
                SELECT symbol, date, open, high, low, close, volume, turnover
                FROM stock_daily
                WHERE symbol = ?
                ORDER BY date
                """,
                conn,
                params=(symbol,),
            )
        return df

    def get_ohlcv_bulk(self, symbols: list[str] | None = None) -> pd.DataFrame:
        """一次读取多只股票的行情，减少策略逐只建立 SQLite 连接的开销。"""
        columns = "symbol, date, open, high, low, close, volume, turnover"
        query = f"SELECT {columns} FROM stock_daily"
        params: list[str] = []
        if symbols:
            placeholders = ",".join("?" for _ in symbols)
            query += f" WHERE symbol IN ({placeholders})"
            params.extend(symbols)
        query += " ORDER BY symbol, date"

        with sqlite3.connect(self.db_path) as conn:
            return pd.read_sql(query, conn, params=params)

    @staticmethod
    def _to_baostock_code(symbol: str) -> str:
        """将股票代码转为 baostock 格式，并正确识别北交所代码。"""
        normalized = str(symbol).strip().lower()
        if "." in normalized:
            exchange, code = normalized.split(".", 1)
            if exchange in {"sh", "sz", "bj"} and code:
                return f"{exchange}.{code}"
            normalized = code

        if normalized.startswith(("4", "8", "920")):
            prefix = "bj"
        elif normalized.startswith(("6", "9")):
            prefix = "sh"
        else:
            prefix = "sz"
        return f"{prefix}.{normalized}"

    # ── 数据同步 ──

    def sync_today_bulk(self) -> int:
        """多进程并行通过 baostock 拉取增量数据（后复权），写入 SQLite。"""
        from datetime import date, timedelta
        from multiprocessing import Pool

        today_str = date.today().strftime("%Y-%m-%d")

        last_dates = self._get_last_dates()
        if not last_dates:
            logger.warning("本地无股票数据，请先执行 --backfill")
            return 0

        tasks = []
        for symbol, last_date in last_dates.items():
            if last_date and last_date >= today_str:
                continue
            start = today_str
            if last_date:
                start = (date.fromisoformat(last_date) + timedelta(days=1)).strftime("%Y-%m-%d")
            tasks.append((symbol, self._to_baostock_code(symbol), start, today_str))

        if not tasks:
            logger.info("所有股票已是最新，无需更新")
            return 0

        logger.info(f"需要更新 {len(tasks)} 只股票，启动多进程并行拉取...")

        # 公共 baostock 服务对并发连接较敏感，4 个 worker 比 8 个更稳。
        n_workers = min(4, len(tasks))
        chunks = [tasks[i::n_workers] for i in range(n_workers)]

        with Pool(n_workers) as pool:
            batch_results = pool.map(_bs_fetch_batch, chunks)

        all_rows = []
        for batch in batch_results:
            all_rows.extend(batch)

        if not all_rows:
            logger.info("无新数据（可能非交易日）")
            return 0

        df = pd.DataFrame(
            all_rows,
            columns=["symbol", "date", "open", "high", "low", "close", "volume", "turnover"],
        )
        for col in ["open", "high", "low", "close", "volume", "turnover"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["close"])
        df = df[df["volume"] > 0]

        count = len(df)
        records = list(
            df[["symbol", "date", "open", "high", "low", "close", "volume", "turnover"]].itertuples(
                index=False, name=None
            )
        )
        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(_UPSERT_SQL, records)
            conn.commit()

        logger.info(f"sync_today_bulk: 写入或更新 {count} 条数据")
        return len(records)

    def sync_market_index(
        self,
        index_codes: dict[str, str] | None = None,
        max_retries: int = 3,
    ) -> int:
        """同步大盘指数日线，默认使用上证指数作为市场状态基准。"""
        from datetime import date, timedelta

        import baostock as bs

        indexes = index_codes or DEFAULT_MARKET_INDEXES
        today_str = date.today().strftime("%Y-%m-%d")
        with sqlite3.connect(self.db_path) as conn:
            last_dates = dict(
                conn.execute(
                    "SELECT index_code, MAX(date) FROM market_daily GROUP BY index_code"
                ).fetchall()
            )

        tasks = []
        for index_code in indexes:
            last_date = last_dates.get(index_code)
            if last_date and last_date >= today_str:
                continue
            start = self.start_date
            if last_date:
                start = (date.fromisoformat(last_date) + timedelta(days=1)).strftime("%Y-%m-%d")
            tasks.append((index_code, start, today_str))

        if not tasks:
            logger.info("大盘指数已是最新，无需更新")
            return 0

        if not _login_baostock(bs):
            logger.error("大盘指数同步登录失败")
            return 0

        records = []
        try:
            for index_code, start, end in tasks:
                rows = _query_history_with_retry(
                    bs,
                    index_code,
                    start,
                    end,
                    adjustflag="3",
                    max_retries=max_retries,
                )
                records.extend((index_code, row[0], *row[1:]) for row in rows)
        finally:
            bs.logout()

        if not records:
            logger.warning("大盘指数没有获取到新数据")
            return 0

        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(_MARKET_UPSERT_SQL, records)
            conn.commit()
        logger.info(f"大盘指数同步完成，写入或更新 {len(records)} 条数据")
        return len(records)

    def get_market_regime(self, index_code: str = "sh.000001") -> pd.DataFrame:
        """按 20/60 日均线和 20 日涨跌幅划分大盘状态。

        优先使用 market_daily 中的真实指数；指数暂时不可用时，
        回退到股票池等权日收益代理，并在 source 列标记来源。
        """
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql(
                """
                SELECT date, close
                FROM market_daily
                WHERE index_code = ?
                ORDER BY date
                """,
                conn,
                params=(index_code,),
            )

        if df.empty:
            with sqlite3.connect(self.db_path) as conn:
                stock_df = pd.read_sql(
                    "SELECT symbol, date, close FROM stock_daily ORDER BY symbol, date",
                    conn,
                )
            if stock_df.empty:
                return stock_df
            stock_df["date"] = pd.to_datetime(stock_df["date"])
            stock_df["daily_return"] = (
                stock_df["close"] / stock_df.groupby("symbol")["close"].shift(1) - 1
            )
            daily = (
                stock_df.groupby("date")["daily_return"]
                .agg(
                    market_return="median",
                    breadth=lambda values: (values > 0).mean(),
                )
                .reset_index()
            )
            daily["close"] = (1 + daily["market_return"].fillna(0)).cumprod() * 100
            df = daily[["date", "close"]]
            source = "stock_equal_weight_proxy"
        else:
            df["date"] = pd.to_datetime(df["date"])
            source = index_code

        df["ma20"] = df["close"].rolling(20).mean()
        df["ma60"] = df["close"].rolling(60).mean()
        df["return20"] = df["close"].pct_change(20)
        df["source"] = source
        df["regime"] = "横盘"
        up = (df["close"] > df["ma60"]) & (df["ma20"] > df["ma60"]) & (df["return20"] >= 0.03)
        down = (df["close"] < df["ma60"]) & (df["ma20"] < df["ma60"]) & (df["return20"] <= -0.03)
        df.loc[up, "regime"] = "上升"
        df.loc[down, "regime"] = "下降"
        df.loc[df[["ma20", "ma60", "return20"]].isna().any(axis=1), "regime"] = "数据不足"
        return df

    def backfill(self, symbols: list[str]) -> None:
        """通过 baostock 批量回填历史日 K 线数据（后复权）。

        容错机制：
        - 单只股票失败自动重试 3 次，间隔递增（2s/4s/8s）
        - 每 200 只股票自动重连 baostock（防止长连接超时）
        - 已入库的自动 skip，中断后可重跑续传
        """
        import time
        from datetime import date, timedelta

        import baostock as bs

        today_str = date.today().strftime("%Y-%m-%d")
        max_retries = 3
        reconnect_interval = 200  # 每处理 N 只股票重连一次

        def _login():
            lg = bs.login()
            if lg.error_code != "0":
                logger.error(f"baostock 登录失败: {lg.error_msg}")
                return False
            return True

        if not _login():
            return

        success = 0
        skipped = 0
        failed = 0
        since_reconnect = 0
        last_dates = self._get_last_dates()
        write_conn = sqlite3.connect(self.db_path)

        try:
            for i, symbol in enumerate(symbols):
                last_date = last_dates.get(symbol)
                if last_date and last_date >= today_str:
                    skipped += 1
                    if (i + 1) % 500 == 0:
                        logger.info(
                            f"已处理 {i + 1}/{len(symbols)}，"
                            f"成功 {success} 跳过 {skipped} 失败 {failed}"
                        )
                    continue

                # 定期重连，防止长连接超时
                since_reconnect += 1
                if since_reconnect >= reconnect_interval:
                    bs.logout()
                    time.sleep(1)
                    if not _login():
                        logger.error("重连失败，终止回填")
                        return
                    since_reconnect = 0

                start = last_date or self.start_date
                if last_date:
                    start = (date.fromisoformat(last_date) + timedelta(days=1)).strftime("%Y-%m-%d")

                bs_code = self._to_baostock_code(symbol)

                # 带重试的查询
                rows = []
                query_ok = False
                for attempt in range(max_retries):
                    try:
                        rs = bs.query_history_k_data_plus(
                            bs_code,
                            "date,open,high,low,close,volume,amount",
                            start_date=start,
                            end_date=today_str,
                            frequency="d",
                            adjustflag="1",  # 后复权
                        )

                        if rs.error_code != "0":
                            raise RuntimeError(rs.error_msg)

                        rows = []
                        while rs.next():
                            rows.append(rs.get_row_data())
                        query_ok = True
                        break

                    except Exception as exc:
                        if attempt < max_retries - 1:
                            wait = 2 ** (attempt + 1)
                            logger.warning(
                                f"[{symbol}] 第{attempt + 1}次失败: {exc}，{wait}s 后重试"
                            )
                            time.sleep(wait)
                            # 重连 baostock
                            bs.logout()
                            time.sleep(1)
                            _login()
                        else:
                            logger.warning(f"[{symbol}] {max_retries}次重试均失败，跳过")

                if not query_ok:
                    failed += 1
                    continue

                if not rows:
                    skipped += 1
                    continue

                df = pd.DataFrame(rows, columns=rs.fields)
                for col in ["open", "high", "low", "close", "volume", "amount"]:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                df = df.dropna(subset=["close"])
                df = df[df["volume"] > 0]

                if df.empty:
                    skipped += 1
                    continue

                df["symbol"] = symbol
                df = df.rename(columns={"amount": "turnover"})
                df = df[["symbol", "date", "open", "high", "low", "close", "volume", "turnover"]]

                records = list(
                    df[
                        ["symbol", "date", "open", "high", "low", "close", "volume", "turnover"]
                    ].itertuples(index=False, name=None)
                )
                write_conn.executemany(_UPSERT_SQL, records)
                if (success + 1) % 100 == 0:
                    write_conn.commit()

                success += 1

                if (i + 1) % 500 == 0:
                    logger.info(
                        f"已处理 {i + 1}/{len(symbols)}，"
                        f"成功 {success} 跳过 {skipped} 失败 {failed}"
                    )

        finally:
            write_conn.commit()
            write_conn.close()
            bs.logout()

        logger.info(f"回填完成 — 成功: {success} | 跳过: {skipped} | 失败: {failed}")

    # ── 股票列表 ──

    def get_all_symbols(self) -> list[str]:
        """通过 baostock 获取全市场 A 股代码列表。"""
        import baostock as bs

        lg = bs.login()
        if lg.error_code != "0":
            logger.error(f"baostock 登录失败: {lg.error_msg}")
            return []

        try:
            rs = bs.query_stock_basic(code_name="", code="")
            field_indexes = {field: index for index, field in enumerate(rs.fields)}
            code_index = field_indexes.get("code", 0)
            name_index = field_indexes.get("code_name", 1)
            type_index = field_indexes.get("type", 4)
            status_index = field_indexes.get("status", 5)
            symbols = []
            name_records = []
            while rs.next():
                row = rs.get_row_data()
                code = row[code_index]
                symbol = code.split(".", 1)[-1]
                stock_type = row[type_index]  # "1" = 股票
                status = row[status_index]  # "1" = 上市
                if status == "1" and stock_type == "1":
                    symbols.append(symbol)
                    name_records.append((symbol, row[name_index]))
            if name_records:
                with sqlite3.connect(self.db_path) as conn:
                    conn.executemany(
                        """
                        INSERT INTO stock_basic (symbol, name, updated_at)
                        VALUES (?, ?, CURRENT_TIMESTAMP)
                        ON CONFLICT(symbol) DO UPDATE SET
                            name = excluded.name,
                            updated_at = CURRENT_TIMESTAMP
                        """,
                        name_records,
                    )
                    conn.commit()
            logger.info(f"获取股票列表完成，共 {len(symbols)} 只")
            return symbols
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            return []
        finally:
            bs.logout()

    def get_local_symbols(self) -> list[str]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT DISTINCT symbol FROM stock_daily ORDER BY symbol"
            ).fetchall()
        return [row[0] for row in rows]
