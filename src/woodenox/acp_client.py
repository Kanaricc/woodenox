from __future__ import annotations

import asyncio
import contextlib
import os
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
from uuid import uuid4

from acp import PROTOCOL_VERSION, RequestError, spawn_agent_process
from acp.schema import (
    AgentMessageChunk,
    AgentPlanContentUpdate,
    AgentPlanRemovedUpdate,
    AgentPlanUpdate,
    AgentThoughtChunk,
    AllowedOutcome,
    AudioContentBlock,
    AvailableCommandsUpdate,
    ClientCapabilities,
    ConfigOptionUpdate,
    CreateElicitationResponse,
    CreateTerminalResponse,
    CurrentModeUpdate,
    DeclineElicitationResponse,
    DeniedOutcome,
    ElicitationMode,
    EmbeddedResourceContentBlock,
    EnvVariable,
    ImageContentBlock,
    Implementation,
    KillTerminalResponse,
    PermissionOption,
    ReadTextFileResponse,
    ReleaseTerminalResponse,
    RequestPermissionResponse,
    ResourceContentBlock,
    SessionInfoUpdate,
    TerminalOutputResponse,
    TextContentBlock,
    ToolCallProgress,
    ToolCallStart,
    ToolCallUpdate,
    UsageUpdate,
    UserMessageChunk,
    WaitForTerminalExitResponse,
    WriteTextFileResponse,
)


class EventSink(Protocol):
    async def progress(
        self, task_id: str, detail: str, plan: list[tuple[str, str]]
    ) -> None: ...

    async def permission(
        self,
        task_id: str,
        request_id: str,
        title: str,
        options: list[PermissionOption],
    ) -> str | None: ...

    async def turn_finished(
        self, task_id: str, stop_reason: str, final_response: str
    ) -> None: ...

    async def crashed(self, task_id: str, error: str) -> None: ...

    async def session_created(self, task_id: str, session_id: str) -> None: ...


@dataclass(slots=True)
class PromptJob:
    text: str
    cwd: Path
    new_session: bool = False


class AcpTaskRunner:
    def __init__(
        self,
        *,
        task_id: str,
        agent_command: Sequence[str],
        cwd: Path,
        sink: EventSink,
        session_id: str | None = None,
    ) -> None:
        self.task_id = task_id
        self.agent_command = tuple(agent_command)
        self.cwd = cwd
        self.sink = sink
        self.session_id = session_id
        self._queue: asyncio.Queue[PromptJob] = asyncio.Queue()
        self._conn: Any = None
        self._running_prompt = False
        self._task: asyncio.Task[None] | None = None
        self._agent_messages: list[str] = []
        self._plan: list[tuple[str, str]] = []
        self._pending_permissions: dict[str, asyncio.Future[str | None]] = {}
        self._permission_options: dict[str, list[PermissionOption]] = {}

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name=f"acp:{self.task_id}")

    async def submit(self, text: str, cwd: Path, *, new_session: bool = False) -> None:
        if self._running_prompt and self._conn is not None:
            self._cancel_permissions()
            await self._conn.cancel(session_id=self.session_id)
        await self._queue.put(PromptJob(text=text, cwd=cwd, new_session=new_session))

    def resolve_permission(self, request_id: str, option_id: str | None) -> bool:
        future = self._pending_permissions.get(request_id)
        if future is None or future.done():
            return False
        future.set_result(option_id)
        return True

    def default_allow_option(self, request_id: str) -> str | None:
        options = self._permission_options.get(request_id, [])
        option = next(
            (item for item in options if str(item.kind) == "allow_once"),
            next((item for item in options if str(item.kind).startswith("allow_")), None),
        )
        return option.option_id if option else None

    def default_reject_option(self, request_id: str) -> str | None:
        options = self._permission_options.get(request_id, [])
        option = next(
            (item for item in options if str(item.kind) == "reject_once"),
            next((item for item in options if str(item.kind).startswith("reject_")), None),
        )
        return option.option_id if option else None

    async def cancel(self) -> None:
        self._cancel_permissions()
        if self._running_prompt and self._conn is not None and self.session_id:
            await self._conn.cancel(session_id=self.session_id)

    async def close(self) -> None:
        await self.cancel()
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

    async def _run(self) -> None:
        client = _AcpClient(self)
        try:
            async with spawn_agent_process(
                client,
                self.agent_command[0],
                *self.agent_command[1:],
                env=os.environ,
            ) as (conn, _process):
                self._conn = conn
                await conn.initialize(
                    protocol_version=PROTOCOL_VERSION,
                    client_capabilities=ClientCapabilities(),
                    client_info=Implementation(
                        name="woodenox", title="Woodenox", version="0.1.0"
                    ),
                )
                await self._open_session(conn)
                while True:
                    job = await self._queue.get()
                    if job.new_session or job.cwd != self.cwd:
                        if self.session_id:
                            with contextlib.suppress(Exception):
                                await conn.close_session(session_id=self.session_id)
                        self.session_id = None
                        self.cwd = job.cwd
                        await self._open_session(conn)
                    self._agent_messages.clear()
                    self._running_prompt = True
                    try:
                        response = await conn.prompt(
                            session_id=self.session_id,
                            prompt=[TextContentBlock(text=job.text)],
                        )
                    finally:
                        self._running_prompt = False
                    await self.sink.turn_finished(
                        self.task_id,
                        str(response.stop_reason),
                        "".join(self._agent_messages).strip(),
                    )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self.sink.crashed(self.task_id, f"{type(exc).__name__}: {exc}")

    async def _open_session(self, conn: Any) -> None:
        if self.session_id:
            try:
                await conn.resume_session(
                    session_id=self.session_id, cwd=str(self.cwd), mcp_servers=[]
                )
                return
            except Exception:
                self.session_id = None
        session = await conn.new_session(cwd=str(self.cwd), mcp_servers=[])
        self.session_id = session.session_id
        await self.sink.session_created(self.task_id, self.session_id)

    async def on_update(self, update: Any) -> None:
        if isinstance(update, AgentMessageChunk):
            if isinstance(update.content, TextContentBlock):
                self._agent_messages.append(update.content.text)
                await self.sink.progress(
                    self.task_id, update.content.text.strip(), self._plan
                )
            return
        if isinstance(update, AgentPlanUpdate):
            self._plan = [(entry.content, str(entry.status)) for entry in update.entries]
            await self.sink.progress(self.task_id, "计划已更新", self._plan)
            return
        if isinstance(update, AgentPlanContentUpdate):
            entries = getattr(update.plan, "entries", None)
            if entries is not None:
                self._plan = [(entry.content, str(entry.status)) for entry in entries]
                await self.sink.progress(self.task_id, "计划已更新", self._plan)
            return
        if isinstance(update, (ToolCallStart, ToolCallProgress)):
            title = update.title or "正在执行工具"
            status = str(update.status or "")
            await self.sink.progress(self.task_id, f"{title} {status}".strip(), self._plan)

    async def on_permission(
        self, tool_call: ToolCallUpdate, options: list[PermissionOption]
    ) -> RequestPermissionResponse:
        request_id = uuid4().hex[:10]
        future: asyncio.Future[str | None] = asyncio.get_running_loop().create_future()
        self._pending_permissions[request_id] = future
        self._permission_options[request_id] = options
        try:
            option_id = await self.sink.permission(
                self.task_id,
                request_id,
                tool_call.title or "Agent 请求执行操作",
                options,
            )
            if option_id is None:
                option_id = await future
            if option_id is None:
                return RequestPermissionResponse(outcome=DeniedOutcome(outcome="cancelled"))
            return RequestPermissionResponse(
                outcome=AllowedOutcome(outcome="selected", optionId=option_id)
            )
        finally:
            self._pending_permissions.pop(request_id, None)
            self._permission_options.pop(request_id, None)

    def _cancel_permissions(self) -> None:
        for future in self._pending_permissions.values():
            if not future.done():
                future.set_result(None)


class _AcpClient:
    def __init__(self, runner: AcpTaskRunner) -> None:
        self.runner = runner

    async def request_permission(
        self,
        session_id: str,
        tool_call: ToolCallUpdate,
        options: list[PermissionOption],
        **kwargs: Any,
    ) -> RequestPermissionResponse:
        return await self.runner.on_permission(tool_call, options)

    async def session_update(
        self,
        session_id: str,
        update: UserMessageChunk
        | AgentMessageChunk
        | AgentThoughtChunk
        | ToolCallStart
        | ToolCallProgress
        | AgentPlanUpdate
        | AgentPlanContentUpdate
        | AgentPlanRemovedUpdate
        | AvailableCommandsUpdate
        | CurrentModeUpdate
        | ConfigOptionUpdate
        | SessionInfoUpdate
        | UsageUpdate,
        **kwargs: Any,
    ) -> None:
        await self.runner.on_update(update)

    async def write_text_file(
        self, session_id: str, path: str, content: str, **kwargs: Any
    ) -> WriteTextFileResponse | None:
        raise RequestError.method_not_found("fs/write_text_file")

    async def read_text_file(
        self,
        session_id: str,
        path: str,
        line: int | None = None,
        limit: int | None = None,
        **kwargs: Any,
    ) -> ReadTextFileResponse:
        raise RequestError.method_not_found("fs/read_text_file")

    async def create_terminal(
        self,
        session_id: str,
        command: str,
        args: list[str] | None = None,
        env: list[EnvVariable] | None = None,
        cwd: str | None = None,
        output_byte_limit: int | None = None,
        **kwargs: Any,
    ) -> CreateTerminalResponse:
        raise RequestError.method_not_found("terminal/create")

    async def terminal_output(
        self, session_id: str, terminal_id: str, **kwargs: Any
    ) -> TerminalOutputResponse:
        raise RequestError.method_not_found("terminal/output")

    async def release_terminal(
        self, session_id: str, terminal_id: str, **kwargs: Any
    ) -> ReleaseTerminalResponse | None:
        raise RequestError.method_not_found("terminal/release")

    async def wait_for_terminal_exit(
        self, session_id: str, terminal_id: str, **kwargs: Any
    ) -> WaitForTerminalExitResponse:
        raise RequestError.method_not_found("terminal/wait_for_exit")

    async def kill_terminal(
        self, session_id: str, terminal_id: str, **kwargs: Any
    ) -> KillTerminalResponse | None:
        raise RequestError.method_not_found("terminal/kill")

    async def create_elicitation(
        self, message: str, mode: ElicitationMode, **kwargs: Any
    ) -> CreateElicitationResponse:
        return DeclineElicitationResponse(action="decline")

    async def complete_elicitation(self, elicitation_id: str, **kwargs: Any) -> None:
        return None

    async def ext_method(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        raise RequestError.method_not_found(method)

    async def ext_notification(self, method: str, params: dict[str, Any]) -> None:
        raise RequestError.method_not_found(method)
