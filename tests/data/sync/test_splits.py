# tests/data/sync/test_splits.py
"""data.sync.splits 单元测试。"""
from unittest.mock import patch, MagicMock
import pytest
import duckdb

from data.store import init_db
from data.writers import upsert_splits
from data.sync.splits import download_splits


SAMPLE_SPLITS = [
    {"id": "abc123", "ticker": "TQQQ", "adjustment_type": "forward_split",
     "execution_date": "2025-11-20", "split_from": 1, "split_to": 2,
     "historical_adjustment_factor": 0.5},
]


@pytest.fixture
def tmp_db(tmp_path):
    db_path = tmp_path / "test.duckdb"
    with patch("config.DB_PATH", db_path), \
         patch("data.store.DB_PATH", db_path):
        init_db()
        yield db_path


def test_download_splits(tmp_db):
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"results": SAMPLE_SPLITS, "status": "OK"}
    mock_resp.raise_for_status = MagicMock()

    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db), \
         patch("data.sync.splits.requests.get", return_value=mock_resp):
        new_splits = download_splits("test_key")

    assert len(new_splits) == 1
    assert new_splits[0]["exec_date"] == "2025-11-20"
    assert new_splits[0]["split_from"] == 1
    assert new_splits[0]["split_to"] == 2

    # 验证写入 DB
    con = duckdb.connect(str(tmp_db))
    row = con.execute("SELECT * FROM splits WHERE ticker = 'TQQQ'").fetchone()
    con.close()
    assert row is not None


def test_download_splits_detects_new(tmp_db):
    """已有记录时，只返回新增的拆股事件"""
    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db):
        upsert_splits([
            {"ticker": "TQQQ", "exec_date": "2025-11-20",
             "split_from": 1, "split_to": 2},
        ])

    api_results = SAMPLE_SPLITS + [
        {"id": "def456", "ticker": "TQQQ", "adjustment_type": "forward_split",
         "execution_date": "2026-06-01", "split_from": 1, "split_to": 3,
         "historical_adjustment_factor": 0.333},
    ]
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"results": api_results, "status": "OK"}
    mock_resp.raise_for_status = MagicMock()

    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db), \
         patch("data.sync.splits.requests.get", return_value=mock_resp):
        new_splits = download_splits("test_key")

    assert len(new_splits) == 1
    assert new_splits[0]["exec_date"] == "2026-06-01"


def test_download_splits_empty(tmp_db):
    """无拆股记录时返回空列表"""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"results": [], "status": "OK"}
    mock_resp.raise_for_status = MagicMock()

    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db), \
         patch("data.sync.splits.requests.get", return_value=mock_resp):
        new_splits = download_splits("test_key")

    assert new_splits == []
