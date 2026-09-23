from __future__ import annotations

import re
import tomllib
from pathlib import Path

from .models import DidaTask, TaskInput

CONFIG_RE = re.compile(r"```woodenox\s*\n(?P<body>.*?)\n```", re.DOTALL | re.IGNORECASE)
STATUS_RE = re.compile(
    r"\n?<!-- woodenox:status:start -->.*?<!-- woodenox:status:end -->\n?",
    re.DOTALL,
)


def parse_task(task: DidaTask) -> TaskInput:
    description = strip_status(task.description)
    match = CONFIG_RE.search(description)
    if match is None:
        raise ValueError('任务描述缺少 ```woodenox 配置块')

    config = tomllib.loads(match.group("body"))
    raw_cwd = config.get("cwd")
    if not isinstance(raw_cwd, str) or not raw_cwd.strip():
        raise ValueError("woodenox 配置块中的 cwd 必须是非空字符串")

    cwd = Path(raw_cwd).expanduser().resolve()
    if not cwd.is_dir():
        raise ValueError(f"cwd 不存在或不是目录：{cwd}")

    prompt_description = CONFIG_RE.sub("", description).strip()
    return TaskInput(task=task, cwd=cwd, prompt_description=prompt_description)


def strip_status(description: str) -> str:
    return STATUS_RE.sub("\n", description).strip()


def render_status(
    description: str,
    *,
    state: str,
    agent: str,
    detail: str = "",
    plan: list[tuple[str, str]] | None = None,
) -> str:
    base = strip_status(description)
    lines = [
        "<!-- woodenox:status:start -->",
        "### Agent 状态",
        "",
        f"- 状态：{state}",
        f"- Agent：{agent}",
    ]
    if detail:
        lines.append(f"- 当前行动：{detail}")
    if plan:
        lines.extend(["", "#### 当前计划", ""])
        marks = {
            "completed": "x",
            "in_progress": ">",
            "pending": " ",
            "cancelled": "-",
        }
        lines.extend(f"- [{marks.get(status, ' ')}] {content}" for content, status in plan)
    lines.append("<!-- woodenox:status:end -->")
    return f"{base}\n\n" + "\n".join(lines)


def initial_prompt(task_input: TaskInput) -> str:
    body = task_input.prompt_description or "（无额外描述）"
    return (
        "请完成下面这个滴答清单任务。先形成计划，再执行任务，并在结束时给出完整的最终回复。\n\n"
        f"任务标题：{task_input.task.title}\n\n"
        f"任务描述：\n{body}\n\n"
        f"工作目录：{task_input.cwd}"
    )
