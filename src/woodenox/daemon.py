from __future__ import annotations

import asyncio
import logging
import re
from pathlib import Path
from typing import Any

from acp.schema import PermissionOption

from .acp_client import AcpTaskRunner
from .dida import DidaClient
from .models import DidaTask, TaskBinding
from .store import Store
from .task_content import initial_prompt, parse_task, render_status

LOGGER = logging.getLogger(__name__)
FIRST_STATUS_INTERVAL = 2 * 60
MAX_STATUS_INTERVAL = 256 * 60
APPROVE_RE = re.compile(r"^woodenox\s+approve\s+(\S+)(?:\s+(\S+))?\s*$", re.I)
REJECT_RE = re.compile(r"^woodenox\s+reject\s+(\S+)\s*$", re.I)


class Daemon:
    def __init__(
        self,
        *,
        dida: DidaClient,
        store: Store,
        project_name: str,
        agent_command: list[str],
        poll_interval: float,
    ) -> None:
        self.dida = dida
        self.store = store
        self.project_name = project_name
        self.agent_command = agent_command
        self.agent_name = Path(agent_command[0]).name
        self.poll_interval = poll_interval
        self.project_id = ""
        self._runners: dict[str, AcpTaskRunner] = {}
        self._status_intervals: dict[str, float] = {}
        self._pending_status: dict[str, tuple[str, str, list[tuple[str, str]]]] = {}
        self._status_tasks: dict[str, asyncio.Task[None]] = {}

    async def run(self) -> None:
        self.project_id = await self._resolve_project()
        LOGGER.info("监控滴答清单 %s (%s)", self.project_name, self.project_id)
        try:
            while True:
                await self._poll()
                await asyncio.sleep(self.poll_interval)
        finally:
            for task in self._status_tasks.values():
                task.cancel()
            await asyncio.gather(*self._status_tasks.values(), return_exceptions=True)
            await asyncio.gather(
                *(runner.close() for runner in self._runners.values()),
                return_exceptions=True,
            )

    async def _resolve_project(self) -> str:
        projects = await self.dida.list_projects()
        matches = [
            project
            for project in projects
            if str(project.get("name") or project.get("title") or "") == self.project_name
        ]
        if len(matches) != 1:
            available = "\n".join(
                "- "
                + str(project.get("name") or project.get("title") or "（未命名）")
                + " ("
                + str(project.get("id") or project.get("projectId") or "无 ID")
                + ")"
                for project in projects
            )
            raise RuntimeError(
                f"清单名称必须精确匹配且唯一：{self.project_name!r}，匹配到 {len(matches)} 个。"
                f"\n可用清单：\n{available or '（没有可用清单）'}"
            )
        project_id = matches[0].get("id") or matches[0].get("projectId")
        if not project_id:
            raise RuntimeError(f"清单缺少 ID：{matches[0]!r}")
        return str(project_id)

    async def _poll(self) -> None:
        tasks = await self.dida.project_undone_tasks(self.project_id)
        undone_ids = {task.id for task in tasks}
        for task in tasks:
            try:
                await self._handle_task(task)
            except Exception:
                LOGGER.exception("处理任务失败：%s", task.id)

        for task_id, runner in list(self._runners.items()):
            if task_id not in undone_ids:
                self._runners.pop(task_id)
                self._cancel_status_update(task_id)
                await runner.close()

    async def _handle_task(self, task: DidaTask) -> None:
        if task.status == -1:
            runner = self._runners.pop(task.id, None)
            if runner:
                await runner.close()
            self._cancel_status_update(task.id)
            self.store.delete(task.id)
            return

        binding = self.store.get(task.id)
        if binding is None:
            try:
                task_input = parse_task(task)
            except ValueError as exc:
                await self._abandon(task, str(exc))
                return

            prompt = initial_prompt(task_input)
            comments = await self.dida.comments(task)
            comment_cursor = next(
                (
                    str(comment.get("id") or comment.get("commentId"))
                    for comment in reversed(comments)
                    if comment.get("id") or comment.get("commentId")
                ),
                "",
            )
            binding = TaskBinding(
                task_id=task.id,
                cwd=str(task_input.cwd),
                session_id=None,
                comment_cursor=comment_cursor,
            )
            self.store.save(binding)
            LOGGER.info("开启任务：%s %s", task.id, task.title)
            runner = self._create_runner(task.id, task_input.cwd, binding)
            await runner.submit(prompt, task_input.cwd)
            await self._write_status(task.id, "执行中", "任务已发送给 Agent", [])
            return

        cwd = Path(binding.cwd)
        if task.id not in self._runners:
            runner = self._create_runner(task.id, cwd, binding)
            await runner.submit(
                "调度器已恢复这个任务。请在原有上下文中继续处理。", cwd
            )
        await self._handle_commands(task, binding)

    def _create_runner(
        self, task_id: str, cwd: Path, binding: TaskBinding
    ) -> AcpTaskRunner:
        runner = AcpTaskRunner(
            task_id=task_id,
            agent_command=self.agent_command,
            cwd=cwd,
            sink=self,
            session_id=binding.session_id,
        )
        runner.start()
        self._runners[task_id] = runner
        return runner

    async def progress(
        self, task_id: str, detail: str, plan: list[tuple[str, str]]
    ) -> None:
        await self._write_status(task_id, "执行中", detail[:300], plan)

    async def permission(
        self,
        task_id: str,
        request_id: str,
        title: str,
        options: list[PermissionOption],
    ) -> str | None:
        binding = self.store.get(task_id)
        if binding is None:
            return None
        task = await self.dida.get_task(task_id, self.project_id)
        choices = "\n".join(
            f"- {option.name}: `{option.option_id}` ({option.kind})" for option in options
        )
        comment = (
            f"[woodenox][permission][{request_id}]\n\n"
            f"Agent 请求确认：{title}\n\n可选项：\n{choices}\n\n"
            f"批准：`woodenox approve {request_id} <option-id>`\n\n"
            f"拒绝：`woodenox reject {request_id}`"
        )
        await self.dida.add_comment(task, comment)
        await self._write_status(
            task_id,
            "等待确认",
            f"{title}；请求编号 {request_id}，请在评论中批准或拒绝",
            [],
        )
        return None

    async def turn_finished(
        self, task_id: str, stop_reason: str, final_response: str
    ) -> None:
        binding = self.store.get(task_id)
        if binding is None or stop_reason == "cancelled":
            return
        if stop_reason != "end_turn":
            await self.crashed(task_id, f"Agent 以 {stop_reason} 停止")
            return

        self._cancel_status_update(task_id)
        task = await self.dida.get_task(task_id, self.project_id)
        final = final_response or "Agent 已结束，但没有返回文本回复。"
        await self.dida.add_comment(task, f"[woodenox][final]\n\n{final}")
        description = render_status(
            task.description,
            state="已完成",
            agent=self.agent_name,
            detail="Agent 已完成任务，最终回复见评论",
        )
        await self.dida.update_description(task, description)
        await self.dida.complete(task)

    async def crashed(self, task_id: str, error: str) -> None:
        binding = self.store.get(task_id)
        if binding is None:
            return
        self._cancel_status_update(task_id)
        task = await self.dida.get_task(task_id, self.project_id)
        await self._abandon(task, error)

    async def _abandon(self, task: DidaTask, error: str) -> None:
        description = render_status(
            task.description,
            state="已放弃",
            agent=self.agent_name,
            detail=error[:500],
        )
        await self.dida.abandon(task, description)
        await self.dida.add_comment(
            task,
            "[woodenox][abandoned]\n\n"
            f"任务执行失败，不会自动重试。\n\n错误：{error}",
        )
        self.store.delete(task.id)

    async def session_created(self, task_id: str, session_id: str) -> None:
        binding = self.store.get(task_id)
        if binding:
            binding.session_id = session_id
            self.store.save(binding)

    async def _write_status(
        self,
        task_id: str,
        state: str,
        detail: str,
        plan: list[tuple[str, str]],
    ) -> None:
        self._pending_status[task_id] = (state, detail, plan)
        if task_id in self._status_tasks:
            return
        interval = self._status_intervals.get(task_id, FIRST_STATUS_INTERVAL)
        self._status_tasks[task_id] = asyncio.create_task(
            self._flush_status_after(task_id, interval),
            name=f"status:{task_id}",
        )

    async def _flush_status_after(self, task_id: str, interval: float) -> None:
        await asyncio.sleep(interval)
        status = self._pending_status.pop(task_id, None)
        if status is None:
            self._status_tasks.pop(task_id, None)
            return
        state, detail, plan = status
        binding = self.store.get(task_id)
        if binding is None:
            self._status_tasks.pop(task_id, None)
            return
        try:
            task = await self.dida.get_task(task_id, self.project_id)
            description = render_status(
                task.description,
                state=state,
                agent=self.agent_name,
                detail=detail,
                plan=plan,
            )
            await self.dida.update_description(task, description)
        except Exception:
            self._pending_status[task_id] = status
            LOGGER.exception("更新任务状态失败：%s", task_id)
        else:
            self._status_intervals[task_id] = min(interval * 2, MAX_STATUS_INTERVAL)
        finally:
            self._status_tasks.pop(task_id, None)
            if task_id in self._pending_status:
                next_interval = self._status_intervals.get(task_id, FIRST_STATUS_INTERVAL)
                self._status_tasks[task_id] = asyncio.create_task(
                    self._flush_status_after(task_id, next_interval),
                    name=f"status:{task_id}",
                )

    def _cancel_status_update(self, task_id: str) -> None:
        pending = self._status_tasks.pop(task_id, None)
        if pending is not None:
            pending.cancel()
        self._pending_status.pop(task_id, None)
        self._status_intervals.pop(task_id, None)

    async def _handle_commands(self, task: DidaTask, binding: TaskBinding) -> None:
        comments = await self.dida.comments(task)
        identified = [
            (str(comment.get("id") or comment.get("commentId")), comment)
            for comment in comments
            if comment.get("id") or comment.get("commentId")
        ]
        start = 0
        if binding.comment_cursor:
            start = next(
                index
                for index, (comment_id, _) in enumerate(identified, start=1)
                if comment_id == binding.comment_cursor
            )

        runner = self._runners[task.id]
        for comment_id, comment in identified[start:]:
            content = str(comment.get("title") or "").strip()
            if content and not content.startswith("[woodenox]"):
                approve = APPROVE_RE.match(content)
                reject = REJECT_RE.match(content)
                if approve:
                    request_id, option_id = approve.groups()
                    if option_id is None:
                        option_id = runner.default_allow_option(request_id)
                    runner.resolve_permission(request_id, option_id)
                elif reject:
                    request_id = reject.group(1)
                    runner.resolve_permission(
                        request_id, runner.default_reject_option(request_id)
                    )
                else:
                    await runner.submit(
                        "用户在滴答清单任务中补充了一条评论：\n\n" + content,
                        Path(binding.cwd),
                    )
            binding.comment_cursor = comment_id
            self.store.save(binding)
