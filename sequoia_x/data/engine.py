"""数据引擎模块：负责 SQLite 行情数据存储与 baostock 增量同步。

本版在保持全部对外函数签名不变的前提下，修复以下问题：

数据正确性
1. 写入统一 UPSERT（ON CONFLICT(symbol, date) DO UPDATE），修复：
   - 旧版 sync_today_bulk 按日期全表 DELETE 会误删其他股票当日数据；
   - 旧版 backfill 用 pandas to_sql 遇 IntegrityError 时整块 500 行静默丢失。
   选 ON CONFLICT DO UPDATE 而非 INSERT OR REPLACE：后者冲突时先 DELETE
   再 INSERT，会改变 rowid、重置未列出列，语义不如显式 UPDATE。
2. sync_today_bulk 对"当日已有数据"的股票仍重拉当日 bar 并覆盖，
   避免盘中运行写入的不完整 K 线永久留在库中（旧版直接 continue）。
3. _to_baostock_code 修复北交所：4/8/920 开头 -> bj，920xxx 不再误判为深市。
4. get_all_symbols 按 rs.fields 动态定位 type/status（旧版注释与下标错位），
   并剔除深/沪 B 股（200/900 开头）。

健壮性
5. 多进程使用 spawn 上下文（避免 fork 继承 baostock 连接状态）；worker 校验
   bs.login() 并回传失败列表，登录失败不再被静默当作"无数据"，也不会因
   抛异常导致整个 Pool.map 崩溃。
6. backfill 维护 login/logout 状态（_logged_in），finally 不会对未登录会话
   调 logout；重连失败立即终止，避免空转；单只股票写入异常被隔离，不中断回填。
7. 连接统一 contextlib.closing 管理（with sqlite3.connect 只管理事务、不关闭
   连接，旧版存在泄漏）；WAL 于初始化时持久化，busy 等待由 timeout 参数提供。
8. backfill 一次性快照 MAX(date)，消除逐只查询的 N+1 问题。

性能与整洁
9. 去掉冗余索引 idx_symbol_date（UNIQUE(symbol, date) 已隐式建索引，最左前缀
   可覆盖 symbol-only 查询），并在初始化时 DROP INDEX IF EXISTS 清理历史库。
10. 去掉 AUTOINCREMENT（INTEGER PRIMARY KEY 已自带自增，省去 sqlite_sequence 维护）。
11. get_ohlcv 显式列名，不再带出自增 id；_write_df 强制按 _COLUMNS 重排列序，
    杜绝调用方传错列。

接口兼容
- _bs_fetch_batch 保留原签名与返回值（list of rows），内部委托给新增的
  _bs_fetch_batch_ex 实现；旧外部调用方可继续使用。
- _get_last_date 单只查询版本保留（内部主流程改用 _get_last_date_map）。
- _CREATE_INDEX_SQL 常量保留定义以兼容外部 import，但内部不再执行。

注意事项
- Windows / macOS（spawn）下，调用 sync_today_bulk 的入口脚本必须加
  if __name__ == "__main__": 保护，否则 multiprocessing 会递归派生进程。
- UPSERT 语法要求 SQLite >= 3.24（Python 3.10+ 自带版本满足）。
"""

import sqlite3
import time
from contextlib import closing
from datetime import date, timedelta
from multiprocessing import get_context
from pathlib import Path

import pandas as pd

from sequoia_x.core.config import Settings
from sequoia_x.core.logger import get_logger

logger = get_logger(__name__)


# ── DDL ──
# INTEGER PRIMARY KEY 本身即 rowid 别名、自带自增，无需 AUTOINCREMENT
_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_daily (
    id       INTEGER PRIMARY KEY,
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

# 兼容保留：旧外部代码若 import 该常量不会失败。
# 内部已不再执行——UNIQUE(symbol, date) 的隐式索引最左前缀已覆盖 symbol 查询，
# 该索引纯属冗余。
_CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_symbol_date ON stock_daily (symbol, date);
"""

# 旧版冗余索引在初始化时清理（全新库 DROP IF EXISTS 无副作用）
_DROP_LEGACY_INDEX_SQL = "DROP INDEX IF EXISTS idx_symbol_date"

_UPSERT_SQL = """
INSERT INTO stock_daily (symbol, date, open, high, low, close, volume, turnover)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(symbol, date) DO UPDATE SET
    open     = excluded.open,
    high     = excluded.high,
    low      = excluded.low,
    close    = excluded.close,
    volume   = excluded.volume,
    turnover = excluded.turnover
"""

_FETCH_FIELDS = "date,open,high,low,close,volume,amount"

# 统一列顺序契约：_write_df 入口会按此重排，防止调用方传错列
_COLUMNS = ["symbol", "date", "open", "high", "low", "close", "volume", "turnover"]


def _bs_fetch_batch_ex(tasks: list) -> tuple[list, list]:
    """多进程 worker 内部实现：独立 login，批量拉取 baostock 数据（后复权）。

    返回 (rows, failed_symbols)：
    - rows 行格式 [symbol, date, open, high, low, close, volume, amount]
    - failed_symbols 为拉取失败的 symbol，主进程告警后下次自动重试
      （进度只由库内 MAX(date) 驱动，失败不会推进进度）。
    """
    import baostock as bs

    lg = bs.login()
    if lg.error_code != "0":
        # 登录失败：整个 chunk 标记失败，绝不静默当"无数据"处理
        return [], [t[0] for t in tasks]

    results: list = []
    failed: list = []
    try:
        for symbol, bs_code, start, end in tasks:
            try:
                rs = bs.query_history_k_data_plus(
                    bs_code,
                    _FETCH_FIELDS,
                    start_date=start,
                    end_date=end,
                    frequency="d",
                    adjustflag="1",  # 后复权：历史值不随后续分红送转变化，增量口径正确
                )
                if rs.error_code != "0":
                    failed.append(symbol)
                    continue
                while rs.next():
                    results.append([symbol] + rs.get_row_data())
            except Exception as exc:  # noqa: BLE001 - 单只股票异常不拖垮整批
                logger.warning(f"[{symbol}] 拉取异常: {exc}")
                failed.append(symbol)
    finally:
        bs.logout()
    return results, failed


def _bs_fetch_batch(tasks: list) -> list:
    """多进程 worker（兼容接口）：独立 login，批量拉取 baostock 数据。

    保留原签名与返回值（成功行列表），供外部旧调用方使用。
    内部流程请使用 _bs_fetch_batch_ex 以获取失败 symbol 信息。
    """
    rows, _failed = _bs_fetch_batch_ex(tasks)
    return rows


class DataEngine:
    """行情数据引擎，负责 SQLite 存储和 baostock 数据同步。"""

    def __init__(self, settings: Settings) -> None:
        self.db_path: str = settings.db_path
        self.start_date: str = settings.start_date
        self._init_db()

    # ── 基础设施 ──

    def _connect(self) -> sqlite3.Connection:
        # timeout 即 SQLite busy_timeout（单位秒）：多进程并发写时自动等待
        return sqlite3.connect(self.db_path, timeout=30)

    def _init_db(self) -> None:
        """初始化数据库，开启 WAL 模式提升并发读写性能。"""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as conn:
            # WAL 持久化到库文件头，仅首次需要；须在事务外执行
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute(_CREATE_TABLE_SQL)
            conn.execute(_DROP_LEGACY_INDEX_SQL)
            conn.commit()
        logger.info(f"数据库初始化完成：{self.db_path}")

    def _get_last_date_map(self) -> dict[str, str]:
        """一次性快照所有 symbol 的最新日期，避免逐只查询（N+1）。"""
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT symbol, MAX(date) FROM stock_daily GROUP BY symbol"
            ).fetchall()
        return {s: d for s, d in rows if d}

    def _get_last_date(self, symbol: str) -> str | None:
        """查询单只股票的最新日期（兼容保留；主流程改用 _get_last_date_map）。"""
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT MAX(date) FROM stock_daily WHERE symbol = ?",
                (symbol,),
            ).fetchone()
        return row[0] if row and row[0] else None

    def get_ohlcv(self, symbol: str) -> pd.DataFrame:
        with closing(self._connect()) as conn:
            df = pd.read_sql(
                "SELECT symbol, date, open, high, low, close, volume, turnover "
                "FROM stock_daily WHERE symbol = ? ORDER BY date",
                conn,
                params=(symbol,),
            )
        return df

    def get_local_symbols(self) -> list[str]:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT DISTINCT symbol FROM stock_daily"
            ).fetchall()
        return [row[0] for row in rows]

    @staticmethod
    def _to_baostock_code(symbol: str) -> str:
        """纯数字代码 → baostock 格式。

        4/8/920 开头 -> bj（北交所：43xxxx/83xxxx/87xxxx/920xxx）；
        6/9 开头     -> sh（60/68 主板科创板、900 沪B）；
        其余         -> sz（00/300/301 深市、200 深B）。
        """
        if symbol.startswith(("4", "8")) or symbol.startswith("920"):
            return f"bj.{symbol}"
        if symbol.startswith(("6", "9")):
            return f"sh.{symbol}"
        return f"sz.{symbol}"

    # ── 通用清洗 / 写入 ──

    @staticmethod
    def _clean_df(df: pd.DataFrame) -> pd.DataFrame:
        """数值化 + 过滤无效行（无 close / volume 或成交量 <= 0）。"""
        for col in _COLUMNS[2:]:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
        df = df.dropna(subset=["close", "volume"])
        return df[df["volume"] > 0].copy()

    def _write_df(self, df: pd.DataFrame) -> int:
        """UPSERT 写入 (symbol, date) 粒度数据。

        幂等：重复写入同键只覆盖对应行，绝不影响其他股票；
        float NaN -> None（SQLite 存 NULL），numpy 数值统一转原生 float。
        """
        df = df[_COLUMNS]  # 强制列顺序，避免调用方传错
        rows = []
        for rec in df.itertuples(index=False, name=None):
            rows.append(
                (
                    str(rec[0]),
                    str(rec[1]),
                    *(float(v) if pd.notna(v) else None for v in rec[2:]),
                )
            )
        with closing(self._connect()) as conn, conn:
            conn.executemany(_UPSERT_SQL, rows)
        return len(rows)

    # ── 数据同步 ──

    def sync_today_bulk(self) -> int:
        """多进程并行增量同步（后复权），UPSERT 写入 SQLite。

        - 以每股 MAX(date) 起增量拉取到今日；
        - 当日已有数据的股票仍会重拉当日 bar 覆盖：防止盘中运行写入
          未收盘的不完整 K 线后被跳过、永远留在库里；
        - 拉取失败的股票不推进进度（进度由库内 MAX(date) 驱动），下次自动重试；
        - 非交易日拉取结果为空，属正常现象。
        """
        today_str = date.today().strftime("%Y-%m-%d")

        last_map = self._get_last_date_map()
        if not last_map:
            logger.warning("本地无股票数据，请先执行 backfill")
            return 0

        tasks = []
        for symbol, last_date in last_map.items():
            if last_date >= today_str:
                # 已有当日数据：仍重拉当日 bar，覆盖盘中可能写入的不完整数据
                start = today_str
            else:
                start = (date.fromisoformat(last_date) + timedelta(days=1)).strftime(
                    "%Y-%m-%d"
                )
            tasks.append((symbol, self._to_baostock_code(symbol), start, today_str))

        if not tasks:
            logger.info("所有股票已是最新，无需更新")
            return 0

        logger.info(f"待处理 {len(tasks)} 只股票，启动多进程并行拉取...")

        n_workers = min(8, len(tasks))
        chunks = [tasks[i::n_workers] for i in range(n_workers)]

        # spawn：避免 fork 继承主进程可能持有的 baostock 连接状态
        ctx = get_context("spawn")
        with ctx.Pool(n_workers) as pool:
            batch_results = pool.map(_bs_fetch_batch_ex, chunks)

        all_rows: list = []
        failed_symbols: list = []
        for rows, bad in batch_results:
            all_rows.extend(rows)
            failed_symbols.extend(bad)
        if failed_symbols:
            logger.warning(
                f"{len(failed_symbols)} 只股票拉取失败（下次运行自动重试），"
                f"示例：{failed_symbols[:10]}"
            )

        if not all_rows:
            logger.info("无新数据（可能非交易日）")
            return 0

        df = pd.DataFrame(all_rows, columns=_COLUMNS)
        df = self._clean_df(df)
        if df.empty:
            logger.info("无有效新数据")
            return 0

        count = self._write_df(df)
        logger.info(f"sync_today_bulk: 处理 {count} 条数据")
        return count

    def backfill(self, symbols: list[str]) -> None:
        """通过 baostock 批量回填历史日 K 线数据（后复权），UPSERT 写入。

        容错机制：
        - 单只股票失败自动重试 3 次，指数退避（2s/4s/8s）并重连；
        - 每 200 只股票自动重连 baostock（防止长连接超时）；
        - 已入库的自动 skip，中断后可重跑续传（UPSERT 保证幂等）；
        - 单只股票写入异常被隔离，不中断整体回填。
        """
        import baostock as bs

        today_str = date.today().strftime("%Y-%m-%d")
        max_retries = 3
        reconnect_interval = 200  # 每处理 N 只股票重连一次

        _logged_in = False

        def _login() -> bool:
            nonlocal _logged_in
            lg = bs.login()
            if lg.error_code != "0":
                logger.error(f"baostock 登录失败: {lg.error_msg}")
                _logged_in = False
                return False
            _logged_in = True
            return True

        def _logout() -> None:
            nonlocal _logged_in
            if _logged_in:
                try:
                    bs.logout()
                finally:
                    _logged_in = False

        # 一次性快照，替代逐只 _get_last_date 的 N+1 查询
        last_map = self._get_last_date_map()

        if not _login():
            return

        success = 0
        skipped = 0
        failed = 0
        since_reconnect = 0

        try:
            for i, symbol in enumerate(symbols):
                last_date = last_map.get(symbol)
                if last_date and last_date >= today_str:
                    skipped += 1
                else:
                    # 定期重连，防止长连接超时
                    since_reconnect += 1
                    if since_reconnect >= reconnect_interval:
                        _logout()
                        time.sleep(1)
                        if not _login():
                            logger.error("重连失败，终止回填")
                            return
                        since_reconnect = 0

                    if last_date:
                        start = (
                            date.fromisoformat(last_date) + timedelta(days=1)
                        ).strftime("%Y-%m-%d")
                    else:
                        start = self.start_date

                    bs_code = self._to_baostock_code(symbol)

                    # 带重试的查询
                    rows: list = []
                    query_ok = False
                    for attempt in range(max_retries):
                        try:
                            rs = bs.query_history_k_data_plus(
                                bs_code,
                                _FETCH_FIELDS,
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
                                _logout()
                                time.sleep(1)
                                if not _login():
                                    # 重连失败说明服务不可用，继续重试只会空转
                                    logger.error(
                                        f"[{symbol}] 重连失败，终止回填 — "
                                        f"成功 {success} 跳过 {skipped} 失败 {failed + 1}"
                                    )
                                    return
                            else:
                                logger.warning(
                                    f"[{symbol}] {max_retries}次重试均失败，跳过"
                                )

                    if not query_ok:
                        failed += 1
                    elif not rows:
                        skipped += 1
                    else:
                        df = pd.DataFrame(
                            rows,
                            columns=[
                                "date", "open", "high", "low", "close",
                                "volume", "amount",
                            ],
                        )
                        df = self._clean_df(df.rename(columns={"amount": "turnover"}))
                        if df.empty:
                            skipped += 1
                        else:
                            df.insert(0, "symbol", symbol)
                            try:
                                self._write_df(df)
                                success += 1
                                # 更新内存缓存，避免中断重跑时重复写入
                                last_map[symbol] = df["date"].max()
                            except Exception as exc:  # noqa: BLE001
                                logger.warning(f"[{symbol}] 写入失败: {exc}")
                                failed += 1

                if (i + 1) % 500 == 0:
                    logger.info(
                        f"已处理 {i + 1}/{len(symbols)}，"
                        f"成功 {success} 跳过 {skipped} 失败 {failed}"
                    )
        finally:
            _logout()

        logger.info(f"回填完成 — 成功: {success} | 跳过: {skipped} | 失败: {failed}")

    # ── 股票列表 ──

    def get_all_symbols(self) -> list[str]:
        """通过 baostock 获取沪深北 A 股代码列表（剔除 B 股）。"""
        import baostock as bs

        lg = bs.login()
        if lg.error_code != "0":
            logger.error(f"baostock 登录失败: {lg.error_msg}")
            return []

        try:
            rs = bs.query_stock_basic(code_name="", code="")
            if rs.error_code != "0":
                logger.error(f"query_stock_basic 失败: {rs.error_msg}")
                return []

            # 字段顺序: code, code_name, ipoDate, outDate, type, status
            # （旧版 row[4]/row[5] 注释写反，这里按字段名动态定位）
            fields = rs.fields
            idx_code = fields.index("code")
            idx_type = fields.index("type")      # 1股票 2指数 3其它
            idx_status = fields.index("status")  # 1上市 0退市

            symbols = []
            while rs.next():
                row = rs.get_row_data()
                if row[idx_type] != "1" or row[idx_status] != "1":
                    continue
                code = row[idx_code].split(".")[1]
                if code.startswith(("200", "900")):  # 深B / 沪B
                    continue
                symbols.append(code)
            logger.info(f"获取股票列表完成，共 {len(symbols)} 只")
            return symbols
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            return []
        finally:
            bs.logout()