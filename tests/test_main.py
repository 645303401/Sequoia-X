"""主程序入口属性测试。"""

from unittest.mock import patch

import pytest
from hypothesis import given
from hypothesis import settings as h_settings
from hypothesis import strategies as st

import main as main_module

# 预先导入 main 模块，避免在 @given 循环中重复导入


# Feature: sequoia-x-v2, Property 13: 主程序异常以非零退出码终止
@given(error_msg=st.text(min_size=1, max_size=100))
@h_settings(max_examples=30, deadline=None)
def test_main_exits_nonzero_on_exception(error_msg: str) -> None:
    """属性 13：main() 中任意未捕获异常应导致 sys.exit(1)。"""
    # patch main 模块中直接引用的 get_settings
    with patch.object(main_module, "get_settings", side_effect=RuntimeError(error_msg)):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()
        assert exc_info.value.code != 0


def test_combined_results_only_return_requested_intersections() -> None:
    results = main_module._build_combined_results(
        {
            "MaVolumeStrategy": ["000001", "000002"],
            "RpsBreakoutStrategy": ["000001", "000003"],
            "TurtleTradeStrategy": ["000001", "000004"],
            "HighTightFlagStrategy": ["000003", "000005"],
            "LimitUpShakeoutStrategy": ["000999"],
        }
    )
    assert results == [
        ("均线放量 + RPS突破 + 海龟突破", ["000001"]),
        ("RPS突破 + 高位旗形整理", ["000003"]),
    ]
