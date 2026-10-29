"""飞书通知模块：将选股结果通过 Webhook 推送至飞书群。"""

import json
import sqlite3
from datetime import date

import requests

from sequoia_x.core.config import Settings
from sequoia_x.core.logger import get_logger

logger = get_logger(__name__)


class FeishuNotifier:
    """飞书 Webhook 推送器。

    根据策略的 webhook_key 路由到对应的飞书机器人。
    若 webhook_key 未在 Settings.strategy_webhooks 中配置，
    则 fallback 到 Settings.feishu_webhook_url。
    """

    def __init__(self, settings: Settings) -> None:
        """
        初始化 FeishuNotifier。

        Args:
            settings: Settings 实例，提供 Webhook URL 配置。
        """
        self.settings = settings

    @staticmethod
    def _to_xueqiu_code(code: str) -> str:
        """将纯数字代码转为雪球格式：6开头→SH，4/8/920开头→BJ，其余→SZ。"""
        if code.startswith("6"):
            return f"SH{code}"
        elif code.startswith(("4", "8", "920")):
            return f"BJ{code}"
        return f"SZ{code}"

    def _get_stock_names(self, symbols: list[str]) -> dict[str, str]:
        """优先从本地 stock_basic 映射表读取名称，缺失时再查询 baostock。"""
        import baostock as bs

        mapping: dict[str, str] = {}
        if not symbols:
            return mapping

        try:
            with sqlite3.connect(self.settings.db_path) as conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS stock_basic (
                        symbol TEXT PRIMARY KEY,
                        name TEXT NOT NULL,
                        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
                placeholders = ",".join("?" for _ in symbols)
                rows = conn.execute(
                    f"SELECT symbol, name FROM stock_basic WHERE symbol IN ({placeholders})",
                    symbols,
                ).fetchall()
            mapping.update(dict(rows))
        except sqlite3.Error as exc:
            logger.warning(f"读取本地股票名称映射失败：{exc}")

        missing_symbols = [symbol for symbol in symbols if symbol not in mapping]
        if not missing_symbols:
            return mapping

        logged_in = False
        try:
            login_result = bs.login()
            if login_result.error_code != "0":
                logger.error(f"股票名称查询登录失败：{login_result.error_msg}")
                return mapping
            logged_in = True
            fetched_records = []
            for code in missing_symbols:
                if code.startswith(("4", "8", "920")):
                    prefix = "bj"
                elif code.startswith(("6", "9")):
                    prefix = "sh"
                else:
                    prefix = "sz"
                rs = bs.query_stock_basic(code=f"{prefix}.{code}")
                while rs.next():
                    row = rs.get_row_data()
                    mapping[code] = row[1]  # 第2个字段是股票名称
                    fetched_records.append((code, row[1]))
            if fetched_records:
                with sqlite3.connect(self.settings.db_path) as conn:
                    conn.executemany(
                        """
                        INSERT INTO stock_basic (symbol, name, updated_at)
                        VALUES (?, ?, CURRENT_TIMESTAMP)
                        ON CONFLICT(symbol) DO UPDATE SET
                            name = excluded.name,
                            updated_at = CURRENT_TIMESTAMP
                        """,
                        fetched_records,
                    )
                    conn.commit()
        except Exception as exc:
            logger.error(f"股票名称查询失败：{exc}")
        finally:
            if logged_in:
                bs.logout()
        return mapping

    def _build_card(self, symbols: list[str], strategy_name: str) -> dict:
        today = date.today().strftime("%Y-%m-%d")
        names = self._get_stock_names(symbols)

        links: list[str] = []
        for code in symbols:
            xq_code = self._to_xueqiu_code(code)
            name = names.get(code, xq_code)
            links.append(f"[{name}](https://xueqiu.com/S/{xq_code})")

        symbol_text = " ".join(links) if links else "（无选股结果）"

        return {
            "msg_type": "interactive",
            "card": {
                "header": {
                    "title": {
                        "tag": "plain_text",
                        "content": f"📈 Sequoia-X 选股播报 | {strategy_name}",
                    },
                    "template": "blue",
                },
                "elements": [
                    {
                        "tag": "div",
                        "text": {
                            "tag": "lark_md",
                            "content": (
                                f"**日期：** {today}\n**策略：** {strategy_name}\n"
                                f"**选股数量：** {len(symbols)}"
                            ),
                        },
                    },
                    {"tag": "hr"},
                    {
                        "tag": "div",
                        "text": {
                            "tag": "lark_md",
                            "content": f"**选股列表：**\n{symbol_text}",
                        },
                    },
                ],
            },
        }

    def _build_combined_card(self, groups: list[tuple[str, list[str]]]) -> dict:
        """构造只包含组合名称、股票代码和股票名称的选股卡片。"""
        symbols = list(dict.fromkeys(code for _, codes in groups for code in codes))
        names = self._get_stock_names(symbols) if symbols else {}
        lines: list[str] = []

        for group_name, group_symbols in groups:
            lines.append(f"**{group_name}**")
            if not group_symbols:
                lines.append("暂无符合股票")
                continue
            lines.extend(f"{code} {names.get(code, '未知名称')}" for code in group_symbols)

        return {
            "msg_type": "interactive",
            "card": {
                "header": {
                    "title": {
                        "tag": "plain_text",
                        "content": "Sequoia-X 组合选股结果",
                    },
                    "template": "blue",
                },
                "elements": [
                    {
                        "tag": "div",
                        "text": {"tag": "lark_md", "content": "\n".join(lines)},
                    }
                ],
            },
        }

    def send_combined_results(
        self,
        groups: list[tuple[str, list[str]]],
        webhook_key: str = "default",
    ) -> None:
        """推送两个组合结果，只展示股票代码和股票名称。"""
        url = self.settings.get_webhook_url(webhook_key)
        payload = self._build_combined_card(groups)

        try:
            resp = requests.post(
                url,
                data=json.dumps(payload),
                headers={"Content-Type": "application/json"},
                timeout=10,
            )
            resp_json = resp.json()
            if resp.status_code != 200 or resp_json.get("code") != 0:
                logger.error(
                    f"组合选股推送失败 [{webhook_key}] "
                    f"HTTP状态={resp.status_code} 飞书响应={resp.text}"
                )
            else:
                logger.info("组合选股推送成功")
        except requests.RequestException as exc:
            logger.error(f"组合选股推送请求异常 [{webhook_key}]：{exc}")

    def send(
        self,
        symbols: list[str],
        strategy_name: str,
        webhook_key: str = "default",
    ) -> None:
        """
        将选股结果格式化为飞书卡片消息并 POST 至对应 Webhook。

        根据 webhook_key 从 Settings 中查找专属 URL；
        若未配置，则 fallback 到 feishu_webhook_url。

        Args:
            symbols: 选股结果代码列表。
            strategy_name: 策略名称，用于卡片标题。
            webhook_key: 策略标识，用于路由到对应飞书机器人。

        Raises:
            不抛出异常，HTTP 失败时记录 ERROR 日志。
        """
        url = self.settings.get_webhook_url(webhook_key)
        payload = self._build_card(symbols, strategy_name)

        try:
            resp = requests.post(
                url,
                data=json.dumps(payload),
                headers={"Content-Type": "application/json"},
                timeout=10,
            )
            # 解析飞书真正的返回体
            resp_json = resp.json()

            # 飞书真正的成功标志是内部的 code == 0
            if resp.status_code != 200 or resp_json.get("code") != 0:
                logger.error(
                    f"飞书推送失败 [{webhook_key}] HTTP状态={resp.status_code} 飞书响应={resp.text}"
                )
            else:
                logger.info(f"飞书推送成功 [{webhook_key}]，共 {len(symbols)} 只股票")

        except requests.RequestException as exc:
            logger.error(f"飞书推送请求异常 [{webhook_key}]：{exc}")
