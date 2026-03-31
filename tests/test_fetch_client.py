import json
import pytest
from unittest.mock import patch, MagicMock
from fetch_client import fetch_hourly_bars, fetch_bars, TIMEFRAME_MAP


def _mock_response(results, next_url=None, status_code=200):
    """构造模拟的 API 响应"""
    resp = MagicMock()
    resp.status_code = status_code
    resp.headers = {"Content-Type": "application/json", "X-Request-Id": "mock-123"}
    body = {
        "ticker": "TQQQ",
        "resultsCount": len(results),
        "results": results,
    }
    if next_url:
        body["next_url"] = next_url
    resp.json.return_value = body
    resp.raise_for_status = MagicMock()
    return resp


SAMPLE_BAR = {
    "o": 50.0,    # open
    "h": 52.0,    # high
    "l": 49.0,    # low
    "c": 51.0,    # close
    "v": 100000,  # volume
    "vw": 50.8,   # vwap
    "n": 500,     # transactions
    "t": 1711627200000,  # 2024-03-28T12:00:00 ET (毫秒时间戳)
}


class TestFetchHourlyBars:
    """测试小时K数据拉取"""

    @patch("fetch_client.requests.get")
    def test_single_page(self, mock_get):
        """单页响应：无 next_url，直接返回全部数据"""
        mock_get.return_value = _mock_response([SAMPLE_BAR])

        bars = fetch_hourly_bars("TQQQ", "2024-03-28", "2026-03-28", api_key="test-key")

        assert len(bars) == 1
        assert bars[0]["o"] == 50.0
        assert bars[0]["t"] == 1711627200000

        # 验证请求 URL 格式
        call_url = mock_get.call_args[0][0]
        assert "/v2/aggs/ticker/TQQQ/range/1/hour/" in call_url

    @patch("fetch_client.time.sleep")
    @patch("fetch_client.requests.get")
    def test_pagination(self, mock_get, mock_sleep):
        """多页响应：通过 next_url 翻页，翻页间有延时"""
        bar1 = {**SAMPLE_BAR, "t": 1711627200000}
        bar2 = {**SAMPLE_BAR, "t": 1711630800000}

        mock_get.side_effect = [
            _mock_response([bar1], next_url="https://api.massive.com/v2/aggs/next?cursor=abc"),
            _mock_response([bar2]),
        ]

        bars = fetch_hourly_bars("TQQQ", "2024-03-28", "2026-03-28", api_key="test-key")

        assert len(bars) == 2
        assert mock_get.call_count == 2
        # 翻页间应有延时
        mock_sleep.assert_called()

    @patch("fetch_client.requests.get")
    def test_api_error_raises(self, mock_get):
        """API 返回错误状态码时抛出异常"""
        resp = MagicMock()
        resp.status_code = 403
        resp.headers = {"Content-Type": "application/json"}
        resp.raise_for_status.side_effect = Exception("403 Forbidden")
        mock_get.return_value = resp

        try:
            fetch_hourly_bars("TQQQ", "2024-03-28", "2026-03-28", api_key="test-key")
            assert False, "应该抛出异常"
        except Exception as e:
            assert "403" in str(e)

    @patch("fetch_client.time.sleep")
    @patch("fetch_client.requests.get")
    def test_429_retry_then_success(self, mock_get, mock_sleep):
        """429 限流后重试成功"""
        resp_429 = MagicMock()
        resp_429.status_code = 429
        resp_429.headers = {"Retry-After": "15"}

        mock_get.side_effect = [
            resp_429,
            _mock_response([SAMPLE_BAR]),
        ]

        bars = fetch_hourly_bars("TQQQ", "2024-03-28", "2026-03-28", api_key="test-key")

        assert len(bars) == 1
        assert mock_get.call_count == 2
        # 限流后应有等待
        mock_sleep.assert_called()

    @patch("fetch_client.time.sleep")
    @patch("fetch_client.requests.get")
    def test_429_exhaust_retries(self, mock_get, mock_sleep):
        """429 限流重试耗尽后抛出异常"""
        resp_429 = MagicMock()
        resp_429.status_code = 429
        resp_429.headers = {"Retry-After": "15"}
        resp_429.raise_for_status.side_effect = Exception("429 Too Many Requests")

        mock_get.return_value = resp_429

        try:
            fetch_hourly_bars("TQQQ", "2024-03-28", "2026-03-28", api_key="test-key")
            assert False, "应该抛出异常"
        except Exception as e:
            assert "429" in str(e)
        # 应该重试了 MAX_RETRIES 次
        assert mock_get.call_count == 5

    @patch("fetch_client.requests.get")
    def test_response_metadata_logged(self, mock_get, caplog):
        """验证响应元数据被记录到日志"""
        import logging
        mock_get.return_value = _mock_response([SAMPLE_BAR])

        with caplog.at_level(logging.DEBUG, logger="fetch_client"):
            fetch_hourly_bars("TQQQ", "2024-03-28", "2026-03-28", api_key="test-key")

        log_text = caplog.text
        # 应记录请求参数
        assert "请求参数" in log_text
        # 应记录响应元数据
        assert "响应元数据" in log_text
        # 应记录首条数据
        assert "首条数据" in log_text
        # 应记录末条数据
        assert "末条数据" in log_text
        # apiKey 不应出现在参数日志中
        assert "test-key" not in log_text


class TestFetchBars:
    """测试多周期K线数据拉取"""

    def test_timeframe_map_keys(self):
        """TIMEFRAME_MAP 包含所有支持的周期"""
        expected = {"hourly", "daily", "weekly", "monthly", "quarterly", "yearly"}
        assert set(TIMEFRAME_MAP.keys()) == expected

    @patch("fetch_client.requests.get")
    def test_daily_url(self, mock_get):
        """daily 周期使用 /range/1/day/ 端点"""
        mock_get.return_value = _mock_response([SAMPLE_BAR])
        fetch_bars("TQQQ", "daily", "2024-03-28", "2026-03-28", api_key="test-key")
        call_url = mock_get.call_args[0][0]
        assert "/v2/aggs/ticker/TQQQ/range/1/day/" in call_url

    @patch("fetch_client.requests.get")
    def test_weekly_url(self, mock_get):
        """weekly 周期使用 /range/1/week/ 端点"""
        mock_get.return_value = _mock_response([SAMPLE_BAR])
        fetch_bars("TQQQ", "weekly", "2024-03-28", "2026-03-28", api_key="test-key")
        call_url = mock_get.call_args[0][0]
        assert "/v2/aggs/ticker/TQQQ/range/1/week/" in call_url

    @patch("fetch_client.requests.get")
    def test_hourly_backward_compat(self, mock_get):
        """hourly 周期与旧 fetch_hourly_bars 行为一致"""
        mock_get.return_value = _mock_response([SAMPLE_BAR])
        bars_new = fetch_bars("TQQQ", "hourly", "2024-03-28", "2026-03-28", api_key="test-key")
        call_url = mock_get.call_args[0][0]
        assert "/v2/aggs/ticker/TQQQ/range/1/hour/" in call_url
        assert len(bars_new) == 1
        assert bars_new[0]["o"] == 50.0

    def test_invalid_timeframe_raises(self):
        """无效周期抛出 ValueError"""
        with pytest.raises(ValueError, match="不支持的周期"):
            fetch_bars("TQQQ", "minutely", "2024-03-28", "2026-03-28", api_key="test-key")

    @patch("fetch_client.time.sleep")
    @patch("fetch_client.requests.get")
    def test_pagination_works_for_daily(self, mock_get, mock_sleep):
        """daily 周期分页逻辑正常"""
        bar1 = {**SAMPLE_BAR, "t": 1711627200000}
        bar2 = {**SAMPLE_BAR, "t": 1711713600000}
        mock_get.side_effect = [
            _mock_response([bar1], next_url="https://api.massive.com/v2/aggs/next?cursor=abc"),
            _mock_response([bar2]),
        ]
        bars = fetch_bars("TQQQ", "daily", "2024-03-28", "2026-03-28", api_key="test-key")
        assert len(bars) == 2
        assert mock_get.call_count == 2
        # 首次请求使用 /range/1/day/
        first_url = mock_get.call_args_list[0][0][0]
        assert "/range/1/day/" in first_url
        mock_sleep.assert_called()
