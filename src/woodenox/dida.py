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
        data = await self._call(
            "get_project_with_undone_tasks", {"project_id": project_id}
        )
        raw_tasks = _find_dict_list(data, ("tasks", "undoneTasks", "data"))
        return [_to_task(item, project_id) for item in raw_tasks]

    async def get_task(self, task_id: str, project_id: str = "") -> DidaTask:
        data = await self._call(
            "get_task_by_id", {"task_id": task_id, "project_id": project_id}
        )
        if isinstance(data, dict):
            for key in ("task", "data", "result"):
                if isinstance(data.get(key), dict):
                    data = data[key]
                    break
        if not isinstance(data, dict):
            raise RuntimeError(f"get_task_by_id 返回了无法识别的数据：{data!r}")
        return _to_task(data, project_id)

    async def update_description(self, task: DidaTask, description: str) -> None:
        await self._call(
            "update_task",
            {
                "task_id": task.id,
                "project_id": task.project_id,
                "title": task.title,
                "description": description,
            },
        )

    async def add_comment(self, task: DidaTask, content: str) -> None:
        await self._call(
            "add_comment",
            {
                "task_id": task.id,
                "project_id": task.project_id,
                "comment": content,
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
        values: dict[str, Any] = {
            "task_id": task.id,
            "project_id": task.project_id,
            "title": task.title,
            "description": description,
        }
        if "update_task" in self._tools and _has_property(self._tools["update_task"], "tags"):
            await self._ensure_tag("woodenox-abandoned")
            values["tags"] = sorted({*task.tags, "woodenox-abandoned"})
        await self._call("update_task", values)

    async def _ensure_tag(self, name: str) -> None:
        if "list_tags" not in self._tools or "create_tag" not in self._tools:
            return
        data = await self._call("list_tags", {})
        tags = _find_dict_list(data, ("tags", "data", "items"))
        if any(str(tag.get("name") or tag.get("label") or "") == name for tag in tags):
            return
        await self._call("create_tag", {"name": name})

    async def _call(self, name: str, values: dict[str, Any]) -> Any:
        if self._client is None:
            raise RuntimeError("DidaClient 尚未连接")
        tool = self._tools.get(name)
        if tool is None:
            raise RuntimeError(f"滴答 MCP 未提供工具：{name}")
        arguments = _tool_arguments(tool, values)
        async with self._lock:
            result = await self._client.call_tool(name, arguments)
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


ALIASES = {
    "task_id": ("taskId", "task_id", "id"),
    "project_id": ("projectId", "project_id"),
    "title": ("title", "content"),
    "description": ("description", "desc"),
    "comment": ("content", "comment", "text"),
    "tags": ("tags", "tagNames"),
    "name": ("name", "tagName"),
}


def _tool_arguments(tool: Any, values: dict[str, Any]) -> dict[str, Any]:
    schema = tool.input_schema
    properties = schema.get("properties", {}) if isinstance(schema, dict) else {}
    arguments: dict[str, Any] = {}
    for semantic_name, value in values.items():
        for candidate in ALIASES.get(semantic_name, (semantic_name,)):
            if candidate in properties:
                arguments[candidate] = value
                break
    return arguments


def _has_property(tool: Any, name: str) -> bool:
    schema = tool.input_schema
    return isinstance(schema, dict) and name in schema.get("properties", {})


def _result_text(result: Any) -> str:
    return "\n".join(
        block.text for block in result.content if getattr(block, "type", None) == "text"
    )


def _find_dict_list(data: Any, keys: Iterable[str]) -> list[dict[str, Any]]:
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if isinstance(data, dict):
        for key in keys:
            value = data.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
            if isinstance(value, dict):
                found = _find_dict_list(value, keys)
                if found:
                    return found
    return []


def _to_task(raw: dict[str, Any], default_project_id: str) -> DidaTask:
    task_id = raw.get("id") or raw.get("taskId") or raw.get("_id")
    if not task_id:
        raise RuntimeError(f"任务缺少 ID：{raw!r}")
    return DidaTask(
        id=str(task_id),
        project_id=str(raw.get("projectId") or raw.get("project_id") or default_project_id),
        title=str(raw.get("title") or raw.get("content") or ""),
        description=str(raw.get("description") or raw.get("desc") or ""),
        status=raw.get("status"),
        tags=[str(tag) for tag in raw.get("tags", [])],
    )
