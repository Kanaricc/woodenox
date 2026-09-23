from __future__ import annotations

import asyncio
import json
from collections.abc import Iterable
from contextlib import AsyncExitStack
from typing import Any

import httpx2
from mcp import Client
from mcp.client.streamable_http import streamable_http_client

from .models import DidaTask


class DidaClient:
    def __init__(self, url: str, token: str) -> None:
        self._url = url
        self._token = token
        self._stack = AsyncExitStack()
        self._client: Client | None = None
        self._tools: dict[str, Any] = {}
        self._lock = asyncio.Lock()

    async def __aenter__(self) -> DidaClient:
        http = httpx2.AsyncClient(
            headers={"Authorization": f"Bearer {self._token}"},
            timeout=httpx2.Timeout(30, read=300),
            follow_redirects=True,
        )
        await self._stack.enter_async_context(http)
        transport = streamable_http_client(self._url, http_client=http)
        self._client = await self._stack.enter_async_context(Client(transport, mode="auto"))
        tools = await self._client.list_tools()
        self._tools = {tool.name: tool for tool in tools.tools}
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self._stack.aclose()

    async def list_projects(self) -> list[dict[str, Any]]:
        data = await self._call("list_projects", {})
        return _find_dict_list(data, ("projects", "lists", "data"))

    async def project_undone_tasks(self, project_id: str) -> list[DidaTask]:
        data = await self._call("get_project_with_undone_tasks", {"project_id": project_id})
        raw_tasks = _find_dict_list(data, ("tasks", "undoneTasks", "data"))
        return [_to_task(item, project_id) for item in raw_tasks]

    async def get_task(self, task_id: str, project_id: str = "") -> DidaTask:
        data = await self._call("get_task_by_id", {"task_id": task_id})
        if isinstance(data, dict):
            for key in ("task", "data", "result"):
                if isinstance(data.get(key), dict):
                    data = data[key]
                    break
        if not isinstance(data, dict):
            raise RuntimeError(f"get_task_by_id 返回了无法识别的数据：{data!r}")
        return _to_task(data, project_id)

    async def create_task(
        self, project_id: str, title: str, description: str
    ) -> DidaTask:
        data = await self._call(
            "create_task",
            {
                "task": {
                    "projectId": project_id,
                    "title": title,
                    "content": description,
                    "kind": "TEXT",
                }
            },
        )
        task_data = _find_dict(data, ("task", "result", "data"))
        return _to_task(task_data, project_id)

    async def update_description(self, task: DidaTask, description: str) -> None:
        description_field = "desc" if task.kind == "CHECKLIST" else "content"
        await self._call(
            "update_task",
            {
                "task_id": task.id,
                "task": {
                    "id": task.id,
                    "projectId": task.project_id,
                    "title": task.title,
                    description_field: description,
                    "kind": task.kind,
                },
            },
        )

    async def add_comment(self, task: DidaTask, content: str) -> None:
        await self._call(
            "add_comment",
            {
                "task_id": task.id,
                "project_id": task.project_id,
                "title": content,
            },
        )

    async def comments(self, task: DidaTask) -> list[dict[str, Any]]:
        data = await self._call(
            "get_comment", {"task_id": task.id, "project_id": task.project_id}
        )
        return _find_dict_list(data, ("comments", "data", "items"))

    async def complete(self, task: DidaTask) -> None:
        await self._call(
            "complete_task", {"task_id": task.id, "project_id": task.project_id}
        )

    async def abandon(self, task: DidaTask, description: str) -> None:
        description_field = "desc" if task.kind == "CHECKLIST" else "content"
        await self._call(
            "update_task",
            {
                "task_id": task.id,
                "task": {
                    "id": task.id,
                    "projectId": task.project_id,
                    "title": task.title,
                    description_field: description,
                    "status": -1,
                    "kind": task.kind,
                },
            },
        )

    async def _call(self, name: str, values: dict[str, Any]) -> Any:
        if self._client is None:
            raise RuntimeError("DidaClient 尚未连接")
        tool = self._tools.get(name)
        if tool is None:
            raise RuntimeError(f"滴答 MCP 未提供工具：{name}")
        async with self._lock:
            result = await self._client.call_tool(name, values)
        if result.is_error:
            raise RuntimeError(_result_text(result) or f"滴答 MCP 工具 {name} 调用失败")
        if result.structured_content is not None:
            return result.structured_content
        text = _result_text(result)
        if not text:
            return None
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return text


def _result_text(result: Any) -> str:
    return "\n".join(
        block.text for block in result.content if getattr(block, "type", None) == "text"
    )


def _find_dict_list(data: Any, keys: Iterable[str]) -> list[dict[str, Any]]:
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if isinstance(data, dict):
        search_keys = tuple(dict.fromkeys((*keys, "result")))
        for key in search_keys:
            value = data.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
            if isinstance(value, dict):
                found = _find_dict_list(value, search_keys)
                if found:
                    return found
    return []


def _find_dict(data: Any, keys: Iterable[str]) -> dict[str, Any]:
    if isinstance(data, dict):
        if data.get("id") or data.get("taskId"):
            return data
        for key in keys:
            value = data.get(key)
            if isinstance(value, dict):
                found = _find_dict(value, keys)
                if found:
                    return found
    raise RuntimeError(f"返回值中没有任务对象：{data!r}")


def _to_task(raw: dict[str, Any], default_project_id: str) -> DidaTask:
    task_id = raw.get("id") or raw.get("taskId") or raw.get("_id")
    if not task_id:
        raise RuntimeError(f"任务缺少 ID：{raw!r}")
    return DidaTask(
        id=str(task_id),
        project_id=str(raw.get("projectId") or raw.get("project_id") or default_project_id),
        title=str(raw.get("title") or raw.get("content") or ""),
        description=str(raw.get("content") or raw.get("desc") or raw.get("description") or ""),
        status=raw.get("status"),
        tags=[str(tag) for tag in (raw.get("tags") or [])],
        kind=str(raw.get("kind") or "TEXT"),
    )
