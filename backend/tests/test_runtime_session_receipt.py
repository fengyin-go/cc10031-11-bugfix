"""定位策略执行记录一致性测试 — 验证回退终态写入修复。

覆盖口径：
1. 回退成功后同一执行记录只保留一个当前终态（success=True 不携带 error）
2. 主定位失败证据保留在审计字段（attempts / primary_error）
3. 最终状态与采用策略同步写入（adopted_strategy）
4. 失败恢复清单只登记最终仍未成功的元素
5. 并发/重复提交只生效一次（幂等）
6. 批量查看执行会话可核对每个元素的最终状态和采用策略
"""
from __future__ import annotations

import random
import sys
from pathlib import Path

import pytest

# 确保 backend 目录在 sys.path 中
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.services.runtime_session import RuntimeSessionService
from app.store import store


def _strategy(rule_id: int = 9001, fallback: str = "xpath=//button[text()='提交']"):
    return {
        "rule_id": rule_id,
        "rule_name": f"提交按钮-{rule_id}",
        "selector_type": "css",
        "expression": "#submit-btn",
        "match_strategy": "first_match",
        "wait_condition": "visible",
        "fallback": fallback,
        "bot_ref": "测试流程",
    }


def _force_random(monkeypatch: pytest.MonkeyPatch, values: list[float]) -> None:
    """按序列替换 random.random，使主/回退定位结果可预测。"""
    seq = iter(values)
    monkeypatch.setattr(random, "random", lambda: next(seq))


# ── 单条执行记录的终态纪律 ──


def test_fallback_success_has_single_terminal_state(monkeypatch):
    """主定位失败 + 回退成功：终态为成功，error 字段不得残留。"""
    svc = RuntimeSessionService()
    # 主定位失败（0.999 > 成功率），回退成功（0.001 < 成功率）
    _force_random(monkeypatch, [0.999, 0.001])

    result = svc._simulate_automation_action(_strategy())

    assert result["success"] is True
    assert result["adopted_strategy"] == "fallback"
    assert "error" not in result, "回退成功后 error 终态字段必须清除"
    # 主失败证据保留在审计字段
    assert result["primary_error"], "主定位失败证据应保留在 primary_error"
    assert len(result["attempts"]) == 2
    assert result["attempts"][0]["strategy"] == "primary"
    assert result["attempts"][0]["success"] is False
    assert result["attempts"][0]["error"], "审计轨迹中应保留主失败原因"
    assert result["attempts"][1]["strategy"] == "fallback"
    assert result["attempts"][1]["success"] is True


def test_primary_success_has_single_terminal_state(monkeypatch):
    """主定位直接成功：终态为成功，采用策略为 primary。"""
    svc = RuntimeSessionService()
    _force_random(monkeypatch, [0.001])

    result = svc._simulate_automation_action(_strategy())

    assert result["success"] is True
    assert result["adopted_strategy"] == "primary"
    assert "error" not in result
    assert "primary_error" not in result
    assert len(result["attempts"]) == 1


def test_final_failure_keeps_error_field(monkeypatch):
    """主定位失败 + 回退失败：终态为失败，error 字段保留。"""
    svc = RuntimeSessionService()
    _force_random(monkeypatch, [0.999, 0.999])

    result = svc._simulate_automation_action(_strategy())

    assert result["success"] is False
    assert result["adopted_strategy"] is None
    assert result["error"], "最终失败必须保留 error 终态字段"
    assert result["primary_error"], "主失败证据应保留"
    assert len(result["attempts"]) == 2


def test_primary_failure_without_fallback(monkeypatch):
    """主定位失败且无回退方案：终态为失败，只有一条尝试记录。"""
    svc = RuntimeSessionService()
    _force_random(monkeypatch, [0.999])

    result = svc._simulate_automation_action(_strategy(fallback=""))

    assert result["success"] is False
    assert result["error"]
    assert len(result["attempts"]) == 1


# ── 会话级执行回执的一致性 ──


def _publish_strategies(*strategies: dict) -> None:
    for s in strategies:
        store.channel.publish(
            topic="rpa.locator_strategies",
            source="element_selector",
            payload=s,
        )


def test_receipt_all_failed_registers_recovery(monkeypatch):
    """全部最终失败：恢复清单登记全部元素，回执计数一致。"""
    svc = RuntimeSessionService()
    monkeypatch.setattr(random, "random", lambda: 0.999)  # 全部尝试失败

    before = store.recovery.count
    _publish_strategies(_strategy(9101), _strategy(9102))
    result = svc.consume_strategies_and_execute()

    assert result["total"] == 2
    assert result["executed"] == 0
    assert result["errors"] == 2
    assert store.recovery.count - before == 2, "最终失败元素必须登记到恢复清单"

    # 每条记录都是单一失败终态
    for r in result["results"]:
        assert r["success"] is False
        assert r["error"]

    # 失败恢复清单只含最终仍未成功的元素
    pending = svc.failure_recovery_list()
    assert len(pending) == 2
    assert {p["rule_id"] for p in pending} == {9101, 9102}

    assert svc.consistency_check()["ok"] is True


def test_receipt_fallback_success_not_in_recovery(monkeypatch):
    """回退成功：计成功不计失败，恢复清单为空，一致性核对通过。"""
    svc = RuntimeSessionService()
    # 每个元素两次尝试：主失败、回退成功
    seq = iter([0.999, 0.001] * 4)
    monkeypatch.setattr(random, "random", lambda: next(seq))

    before = store.recovery.count
    _publish_strategies(_strategy(9201), _strategy(9202))
    result = svc.consume_strategies_and_execute()

    assert result["executed"] == 2
    assert result["errors"] == 0
    assert result["fallback_recovered"] == 2
    assert store.recovery.count == before, "回退成功不得登记失败恢复清单"

    for r in result["results"]:
        assert r["success"] is True
        assert "error" not in r, "成功记录不得携带 error 字段"
        assert r["adopted_strategy"] == "fallback"
        assert r["primary_error"], "主失败证据应保留供审计"

    assert svc.failure_recovery_list() == []
    check = svc.consistency_check()
    assert check["ok"] is True, f"一致性核对失败: {check['violations']}"


def test_duplicate_submission_only_effective_once(monkeypatch):
    """并发重复提交：同一规则在同一会话中只执行一次。"""
    svc = RuntimeSessionService()
    monkeypatch.setattr(random, "random", lambda: 0.001)  # 全部成功

    _publish_strategies(_strategy(9301), _strategy(9301), _strategy(9302))
    result = svc.consume_strategies_and_execute()

    assert result["total"] == 2, "重复规则只应执行一次"
    assert result["duplicates_skipped"] == 1
    assert result["executed"] == 2

    # 同一会话中同一元素只出现一次
    rule_ids = [r["rule_id"] for r in result["results"]]
    assert sorted(rule_ids) == [9301, 9302]
    assert svc.consistency_check()["ok"] is True


def test_second_consume_is_noop(monkeypatch):
    """已确认的消息不会被重复消费（跨调用幂等）。"""
    svc = RuntimeSessionService()
    monkeypatch.setattr(random, "random", lambda: 0.001)

    _publish_strategies(_strategy(9401))
    first = svc.consume_strategies_and_execute()
    assert first["total"] == 1

    second = svc.consume_strategies_and_execute()
    assert second["executed"] == 0
    assert second["session_id"] is None


def test_retry_success_clears_recovery_entry(monkeypatch):
    """跨会话重试成功：失败恢复清单以最新终态为准，不再展示该元素。"""
    svc = RuntimeSessionService()

    # 第一次会话：全部失败
    monkeypatch.setattr(random, "random", lambda: 0.999)
    _publish_strategies(_strategy(9501))
    first = svc.consume_strategies_and_execute()
    assert first["errors"] == 1
    assert len(svc.failure_recovery_list()) == 1

    # 第二次会话：同一元素重试成功
    monkeypatch.setattr(random, "random", lambda: 0.001)
    _publish_strategies(_strategy(9501))
    second = svc.consume_strategies_and_execute()
    assert second["executed"] == 1

    assert svc.failure_recovery_list() == [], "重试成功后不得再出现在失败恢复清单"
    assert svc.consistency_check()["ok"] is True


# ── 批量会话查看 ──


def test_batch_session_detail_shows_final_state_and_strategy(monkeypatch):
    """批量选择执行会话：每个元素展示最终状态和采用策略。"""
    svc = RuntimeSessionService()
    seq = iter([0.001, 0.999, 0.999])  # 元素1主定位成功；元素2主失败+回退失败
    monkeypatch.setattr(random, "random", lambda: next(seq))

    _publish_strategies(_strategy(9601), _strategy(9602))
    result = svc.consume_strategies_and_execute()
    sid = result["session_id"]

    detail = svc.get_sessions_detail([sid, "不存在的会话"])
    assert detail["requested"] == 2
    assert detail["found"] == 1

    elements = detail["sessions"][0]["elements"]
    assert len(elements) == 2

    e1 = next(e for e in elements if e["rule_id"] == 9601)
    assert e1["final_status"] == "成功"
    assert e1["adopted_strategy"] == "primary"
    assert e1["error"] is None

    e2 = next(e for e in elements if e["rule_id"] == 9602)
    assert e2["final_status"] == "失败"
    assert e2["adopted_strategy"] is None
    assert e2["error"], "最终失败元素必须展示错误信息"


def test_receipt_payload_publishes_consistent_dataset(monkeypatch):
    """执行回执：最终状态与采用策略随结果集同步发布到通道。"""
    svc = RuntimeSessionService()
    seq = iter([0.999, 0.001])  # 主失败 + 回退成功
    monkeypatch.setattr(random, "random", lambda: next(seq))

    before = len(store.channel.snapshot(topic="rpa.automation_executed"))
    _publish_strategies(_strategy(9701))
    svc.consume_strategies_and_execute()

    events = store.channel.snapshot(topic="rpa.automation_executed")
    assert len(events) == before + 1
    payload = events[-1]["payload"]
    assert payload["successful"] == 1
    assert payload["failed"] == 0
    assert payload["fallback_recovered"] == 1

    record = payload["results"][0]
    # 同一条执行记录只保留一个当前终态
    assert record["success"] is True
    assert "error" not in record
    assert record["adopted_strategy"] == "fallback"
    assert record["primary_error"], "主失败证据随回执保留供审计"
