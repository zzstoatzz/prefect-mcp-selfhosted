import json
import os

import pytest

from prefect_mcp_server.chaos_status import inspect_chaos


def test_latched_budget_and_recent_results(tmp_path):
    history = [{"started": 100_000 + index, "seconds": 100} for index in range(24)]
    encoded = json.dumps(history)
    (tmp_path / "history.json").write_text(encoded)
    (tmp_path / "STOPPED.json").write_text(
        json.dumps({"failure": "event replay timeout"})
    )
    (tmp_path / "active.json").write_text(json.dumps({"scenario": "event-overlap"}))
    for index in range(5):
        report = tmp_path / "reports" / f"10000{index}-85"
        report.mkdir(parents=True)
        (report / "result.json").write_text(
            json.dumps({"passed": False, "seed": index})
        )
    result = inspect_chaos(tmp_path, now=100_050)
    assert result["status"] == "latched"
    assert result["stop_record"]["failure"] == "event replay timeout"
    assert result["budget_available"] is False
    assert result["budget"]["supervised_budget_available"] is True
    assert result["budget"]["not_before"] == 186_400
    assert result["budget"]["runtime_seconds_last_24h"] == 2400
    assert [item["seed"] for item in result["recent_results"]] == [4, 3, 2]
    assert result["runtime_observed"] is False
    assert (tmp_path / "history.json").read_text() == encoded
    later = inspect_chaos(tmp_path, now=186_400)
    assert later["budget_available"] is True
    assert later["status"] == "latched"


def test_duration_budget_requires_enough_entries_to_expire(tmp_path):
    (tmp_path / "history.json").write_text(
        json.dumps(
            [
                {"started": 1, "seconds": 10},
                {"started": 100_000, "seconds": 100},
                {"started": 100_010, "seconds": 3200},
            ]
        )
    )
    result = inspect_chaos(tmp_path, now=100_050)
    assert result["budget"]["experiments_last_24h"] == 2
    assert result["budget"]["not_before"] == 186_410
    assert result["budget"]["supervised_budget_available"] is False


def test_supervised_count_limit_shares_history(tmp_path):
    encoded = json.dumps([{"started": 100_000, "seconds": 10}] * 28)
    (tmp_path / "history.json").write_text(encoded)
    result = inspect_chaos(tmp_path, now=100_050)
    assert result["budget"]["supervised_budget_available"] is False
    assert result["budget"]["supervised_experiment_limit"] == 28
    assert (tmp_path / "history.json").read_text() == encoded


@pytest.mark.parametrize("content", ["{", "{}", '[{"started": 1, "seconds": -1}]'])
def test_corrupt_history_is_unknown(tmp_path, content):
    (tmp_path / "history.json").write_text(content)
    assert inspect_chaos(tmp_path)["status"] == "unknown"


def test_symlink_and_oversized_record_are_not_read(tmp_path):
    (tmp_path / "history.json").write_text("[]")
    outside = tmp_path / "outside"
    outside.write_text('{"reason": "must not be returned"}')
    stop = tmp_path / "STOPPED.json"
    stop.symlink_to(outside)
    result = inspect_chaos(tmp_path)
    assert result["status"] == "unknown"
    assert "stop_record" not in result
    stop.unlink()
    with stop.open("wb") as file:
        file.truncate(2 * 1024**2 + 1)
    assert inspect_chaos(tmp_path)["status"] == "unknown"


def test_missing_state_is_unknown(tmp_path):
    assert inspect_chaos(tmp_path / "missing")["status"] == "unknown"


def test_fifo_is_rejected_without_waiting_for_a_writer(tmp_path):
    os.mkfifo(tmp_path / "history.json")
    assert inspect_chaos(tmp_path)["status"] == "unknown"


def test_excessive_history_is_bounded(tmp_path):
    (tmp_path / "history.json").write_text(json.dumps([{}] * 513))
    result = inspect_chaos(tmp_path)
    assert result["status"] == "unknown"
    assert "512-entry" in result["error"]
