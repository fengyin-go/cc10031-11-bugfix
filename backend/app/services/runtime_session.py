"""运行时会话服务 — 消费定位策略并执行自动化操作。

这是新建的跨模块消费服务（不存在于 spec 的独立模块），
是元素定位链路的末端消费者。

数据流:
    channel("rpa.locator_strategies") ← element_selector
    → runtime_session 消费 → 模拟页面自动化执行
    → channel("rpa.automation_executed") → execution_log
"""
from __future__ import annotations

import random
import uuid
from datetime import datetime, timezone
from typing import Any

from app.store import store


class RuntimeSessionService:
    """运行时会话管理 — 不继承 BaseModuleService，是纯跨模块消费者。"""

    SERVICE_NAME = "runtime_session"

    def __init__(self) -> None:
        self._sessions: dict[str, dict[str, Any]] = {}

    def consume_strategies_and_execute(self) -> dict[str, Any]:
        """消费定位策略并执行模拟自动化操作。

        从 channel("rpa.locator_strategies") 获取策略，
        模拟在浏览器会话中应用定位规则执行自动化。

        执行回执纪律（定位策略执行记录数据集的生成口径）：
        - 同一规则在同一会话中只执行一次，并发/重复提交只生效一次；
        - 会话统计、结果清单、失败恢复登记全部以最终状态为准，
          最终状态与采用策略在回执定稿时同步写入；
        - 失败恢复清单只登记最终仍未成功的元素。

        Returns:
            {"executed": int, "session_id": str, "results": [...]}
        """
        msgs = store.channel.consume(
            consumer=self.SERVICE_NAME,
            topic="rpa.locator_strategies",
        )
        if not msgs:
            return {
                "executed": 0,
                "session_id": None,
                "total": 0,
                "errors": 0,
                "fallback_recovered": 0,
                "duplicates_skipped": 0,
                "results": [],
            }

        session_id = str(uuid.uuid4())[:8]
        session = {
            "session_id": session_id,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "strategies_applied": 0,
            "actions_executed": 0,
            "errors": 0,
            "fallback_recovered": 0,
            "duplicates_skipped": 0,
        }
        results: list[dict[str, Any]] = []
        seen_rule_ids: set[Any] = set()

        for msg in msgs:
            strategy = msg.payload
            rule_id = strategy.get("rule_id")

            # 幂等：同一规则重复提交只生效一次，多余消息直接确认跳过
            if rule_id is not None and rule_id in seen_rule_ids:
                session["duplicates_skipped"] += 1
                store.channel.confirm(msg.msg_id, self.SERVICE_NAME)
                continue
            if rule_id is not None:
                seen_rule_ids.add(rule_id)

            action_result = self._simulate_automation_action(strategy)
            action_result["rule_id"] = rule_id
            action_result["session_id"] = session_id

            # 按最终状态统计：回退成功只计成功，不再重复计失败
            if action_result["success"]:
                session["actions_executed"] += 1
                if action_result.get("adopted_strategy") == "fallback":
                    session["fallback_recovered"] += 1
            else:
                session["errors"] += 1
                # 失败恢复清单只登记最终仍未成功的元素
                store.recovery.log(
                    module=self.SERVICE_NAME,
                    batch_ref=session_id,
                    step=f"apply_locator:{strategy.get('rule_name', 'unknown')}",
                    error_detail=action_result.get("error", "unknown"),
                )

            session["strategies_applied"] += 1
            results.append(action_result)
            store.channel.confirm(msg.msg_id, self.SERVICE_NAME)

        # 发布执行回执：最终状态与采用策略随结果集同步发布
        store.channel.publish(
            topic="rpa.automation_executed",
            source=self.SERVICE_NAME,
            payload={
                "session_id": session_id,
                "total_strategies": session["strategies_applied"],
                "successful": session["actions_executed"],
                "failed": session["errors"],
                "fallback_recovered": session["fallback_recovered"],
                "duplicates_skipped": session["duplicates_skipped"],
                "results": results,
            },
        )

        # 处理列表与结果清单同一定稿：会话摘要和逐元素结果一起落库
        session["finished_at"] = datetime.now(timezone.utc).isoformat()
        session["results"] = results
        self._sessions[session_id] = session
        return {
            "executed": session["actions_executed"],
            "session_id": session_id,
            "total": session["strategies_applied"],
            "errors": session["errors"],
            "fallback_recovered": session["fallback_recovered"],
            "duplicates_skipped": session["duplicates_skipped"],
            "results": results,
        }

    def _simulate_automation_action(self, strategy: dict[str, Any]) -> dict[str, Any]:
        """模拟自动化操作执行（模拟浏览器元素交互）。

        终态写入规则：
        - 主定位失败先把失败证据写入 attempts 审计轨迹；
        - 回退定位执行完毕后，最终状态与采用策略在同一步骤同步定稿；
        - 同一条执行记录只保留一个当前终态：success=True 时不得
          携带有效 error 字段，主失败证据只留在 attempts/primary_error
          审计字段中，不占用 error 终态字段。

        Args:
            strategy: 定位策略

        Returns:
            {"success": bool, "adopted_strategy": str | None,
             "action": str, "attempts": [...], ...}
        """
        selector_type = strategy.get("selector_type", "css")
        expression = strategy.get("expression", "")
        rule_name = strategy.get("rule_name")
        wait = strategy.get("wait_condition", "visible")

        attempts: list[dict[str, Any]] = []

        # 第一次尝试：主定位
        primary = self._run_locator_attempt(
            leg="primary", selector_type=selector_type,
            expression=expression, wait=wait)
        attempts.append(primary)

        # 第二次尝试：主定位失败且配置了回退方案时执行回退定位
        fallback_expr = strategy.get("fallback", "")
        if not primary["success"] and fallback_expr:
            fallback = self._run_locator_attempt(
                leg="fallback", selector_type=selector_type,
                expression=fallback_expr, wait=wait)
            attempts.append(fallback)

        # ---- 终态定稿：最终状态与采用策略同步写入，只产生一个当前终态 ----
        final = attempts[-1]
        final_success = bool(final["success"])
        adopted = final["strategy"] if final_success else None

        result: dict[str, Any] = {
            "rule_name": rule_name,
            "selector_type": selector_type,
            "expression": expression[:50],
            "success": final_success,
            "adopted_strategy": adopted,
            "attempts": attempts,
            "duration_ms": sum(a["duration_ms"] for a in attempts),
        }

        if final_success:
            result["element_found"] = True
            if adopted == "fallback":
                result["action"] = f"回退定位成功: {fallback_expr[:30]}"
                # 主失败证据保留在独立审计字段，供审计查询
                result["primary_error"] = primary.get("error")
            else:
                result["action"] = f"定位成功: {rule_name or 'N/A'}"
        else:
            result["action"] = "定位失败"
            result["error"] = final.get("error", "unknown")
            if len(attempts) > 1:
                result["primary_error"] = primary.get("error")

        return result

    def _run_locator_attempt(self, leg: str, selector_type: str,
                             expression: str, wait: str) -> dict[str, Any]:
        """执行单次定位尝试（主定位或回退定位的一条腿）。

        Args:
            leg: "primary" 或 "fallback"
            selector_type: 选择器类型
            expression: 本次尝试使用的定位表达式
            wait: 等待条件

        Returns:
            {"strategy": leg, "expression": str, "success": bool,
             "duration_ms": int, "error"?: str}
        """
        # 模拟不同定位策略的成功率
        success_rates = {"id": 0.95, "css": 0.85, "xpath": 0.80, "text": 0.75}
        base_rate = success_rates.get(selector_type, 0.80)

        # 等待条件影响成功率
        if wait == "visible":
            base_rate += 0.05
        elif wait == "none":
            base_rate -= 0.10

        # 回退定位作为兜底路径，命中率独立折算
        if leg == "fallback":
            base_rate = max(base_rate - 0.35, 0.05)

        success = random.random() < base_rate
        attempt: dict[str, Any] = {
            "strategy": leg,
            "expression": expression[:50],
            "success": success,
            "duration_ms": random.randint(50, 500),
        }
        if not success:
            attempt["error"] = self._generate_error(selector_type, expression)
        return attempt

    @staticmethod
    def _generate_error(selector_type: str, expression: str) -> str:
        errors = [
            f"选择器超时: 元素 {expression[:30]} 在等待时间内未出现",
            "元素被遮挡，无法交互",
            f"页面动态加载导致 {selector_type} 选择器失效",
            "元素在 iframe 内，需切换上下文",
        ]
        return random.choice(errors)

    def list_sessions(self) -> list[dict[str, Any]]:
        """列出所有运行中的会话（摘要，不含逐元素结果）。"""
        return [
            {k: v for k, v in session.items() if k != "results"}
            for session in self._sessions.values()
        ]

    def get_session(self, session_id: str) -> dict[str, Any] | None:
        return self._sessions.get(session_id)

    def get_sessions_detail(self, session_ids: list[str]) -> dict[str, Any]:
        """批量查看执行会话：每个元素的最终状态和采用策略。

        Args:
            session_ids: 批量选中的执行会话 ID 列表

        Returns:
            {"requested": int, "found": int, "sessions": [...]}
        """
        sessions = []
        for sid in session_ids:
            session = self._sessions.get(sid)
            if session is None:
                continue
            sessions.append({
                "session_id": sid,
                "started_at": session.get("started_at"),
                "finished_at": session.get("finished_at"),
                "elements": [
                    {
                        "rule_id": r.get("rule_id"),
                        "rule_name": r.get("rule_name"),
                        "final_status": "成功" if r.get("success") else "失败",
                        "adopted_strategy": r.get("adopted_strategy"),
                        "error": r.get("error"),
                        "primary_error": r.get("primary_error"),
                    }
                    for r in session.get("results", [])
                ],
            })
        return {
            "requested": len(session_ids),
            "found": len(sessions),
            "sessions": sessions,
        }

    def failure_recovery_list(self) -> list[dict[str, Any]]:
        """失败恢复清单：解析定位策略执行记录，只登记最终仍未成功的元素。

        同一元素跨会话重试时以其最新一条执行记录的终态为准；
        回退已成功或后续重试成功的元素不出现在清单中。
        """
        latest_by_element: dict[Any, dict[str, Any]] = {}
        for session in self._sessions.values():
            for r in session.get("results", []):
                key = r.get("rule_id") if r.get("rule_id") is not None \
                    else r.get("rule_name")
                latest_by_element[key] = r

        return [
            {
                "session_id": r.get("session_id"),
                "rule_id": r.get("rule_id"),
                "rule_name": r.get("rule_name"),
                "final_status": "失败",
                "error": r.get("error"),
            }
            for r in latest_by_element.values()
            if not r.get("success")
        ]

    def consistency_check(self) -> dict[str, Any]:
        """核对执行记录一致性：同一元素不得同时显示成功和失败。

        检查两类冲突：
        - 互斥终态共存：同一记录 success=True 却携带有效 error 字段；
        - 成功失败重复登记：同一元素在同一会话中同时进入成功与失败清单。
        """
        violations: list[dict[str, Any]] = []
        for session in self._sessions.values():
            sid = session.get("session_id")
            seen: dict[Any, bool] = {}
            for r in session.get("results", []):
                key = r.get("rule_id") if r.get("rule_id") is not None \
                    else r.get("rule_name")
                if r.get("success") and r.get("error"):
                    violations.append({
                        "type": "互斥终态共存",
                        "session_id": sid,
                        "element": key,
                        "detail": "success=True 同时携带 error 字段",
                    })
                if key in seen and seen[key] != bool(r.get("success")):
                    violations.append({
                        "type": "成功失败重复登记",
                        "session_id": sid,
                        "element": key,
                        "detail": "同一元素在成功列表与失败清单中重复出现",
                    })
                seen[key] = bool(r.get("success"))
        return {
            "ok": not violations,
            "violation_count": len(violations),
            "violations": violations,
            "sessions_checked": len(self._sessions),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        return {
            "module": self.SERVICE_NAME,
            "entity": "运行时会话",
            "artifact": "自动化执行记录",
            "active_sessions": len(self._sessions),
            "actions_executed": sum(
                s.get("actions_executed", 0) for s in self._sessions.values()),
            "fallback_recovered": sum(
                s.get("fallback_recovered", 0) for s in self._sessions.values()),
            "pending_recovery": len(self.failure_recovery_list()),
            "consistency_ok": self.consistency_check()["ok"],
            "automation_events": len(
                store.channel.snapshot(topic="rpa.automation_executed")),
        }


# 全局运行时实例
runtime_session = RuntimeSessionService()