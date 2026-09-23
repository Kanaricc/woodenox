from __future__ import annotations

import argparse
import asyncio
import logging
import os
from pathlib import Path

from dotenv import load_dotenv

from .daemon import Daemon
from .dida import DidaClient
from .store import Store


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="woodenox")
    commands = root.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="监控滴答清单并运行 ACP Agent")
    run.add_argument("--project", required=True, help="滴答清单名称，必须精确匹配")
    run.add_argument("--agent", required=True, help="ACP Agent 可执行文件")
    run.add_argument("--dida-url", default="https://mcp.dida365.com")
    run.add_argument("--token-env", default="DIDA365_TOKEN")
    run.add_argument("--poll-interval", type=float, default=5.0)
    run.add_argument("--state-db", type=Path, default=Path(".woodenox/state.db"))
    run.add_argument("--verbose", action="store_true")
    run.add_argument(
        "agent_args",
        nargs=argparse.REMAINDER,
        help="`--` 之后的内容原样传给 Agent",
    )
    return root


def main() -> None:
    args = parser().parse_args()
    load_dotenv()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    token = os.environ.get(args.token_env)
    if not token:
        raise SystemExit(f"环境变量 {args.token_env} 未设置")
    asyncio.run(_run(args, token))


async def _run(args: argparse.Namespace, token: str) -> None:
    agent_args = args.agent_args[1:] if args.agent_args[:1] == ["--"] else args.agent_args
    store = Store(args.state_db.resolve())
    async with DidaClient(args.dida_url, token) as dida:
        daemon = Daemon(
            dida=dida,
            store=store,
            project_name=args.project,
            agent_command=[args.agent, *agent_args],
            poll_interval=args.poll_interval,
        )
        await daemon.run()
