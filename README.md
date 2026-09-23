# woodenox

`woodenox` 将滴答清单中的未完成任务发送给 ACP Agent，并把 Agent 的计划、当前行动和最终回复同步回任务。

## 任务格式

每个任务必须在描述中包含一个 `woodenox` 配置块。`cwd` 从任务内容中读取，不由守护进程统一指定。

````markdown
```woodenox
cwd = "/Users/me/projects/example"
```

实现登录接口，并更新相关文档。
````

配置块不会发送给 Agent。`cwd` 可以使用绝对路径或 `~`，启动 Agent 前会解析为绝对路径并确认目录存在。

任务标题、描述和 `cwd` 只在创建 Agent 时读取一次。Agent 创建后，woodenox 不再检查用户是否修改了任务标题、描述或 `cwd`，结束前也不会进行二次比较。

Agent 执行期间新增的普通任务评论会作为消息发送到当前 ACP session。后续要求应通过评论补充。

Agent 的计划和当前行动会写入任务描述。第一次在 2 分钟后更新，后续间隔依次为 4、8、16 分钟，最大 256 分钟；每次只写入期间最新的状态。Agent 结束或失败时会立即更新最终状态。

## 运行

先在滴答清单网页版的「账户与安全」中创建 API 口令，并在项目根目录创建 `.env`：

```dotenv
DIDA365_TOKEN=...
```

也可以直接设置同名进程环境变量；进程环境变量优先于 `.env`。

启动监控：

```bash
uv run woodenox run \
  --project "研发" \
  --agent codex-acp
```

需要向 Agent 传参时，把参数放在 `--` 后面：

```bash
uv run woodenox run \
  --project "研发" \
  --agent my-agent \
  -- --profile work
```

其他参数：

- `--poll-interval`：轮询间隔，默认 5 秒。
- `--state-db`：SQLite 状态库，默认 `.woodenox/state.db`。
- `--dida-url`：滴答 MCP 地址，默认 `https://mcp.dida365.com`。
- `--token-env`：Token 环境变量名，默认 `DIDA365_TOKEN`。

## 权限确认

Agent 发出 ACP 权限请求后，任务会显示“等待确认”，并新增一条包含请求编号和可选项的评论。

批准一次：

```text
woodenox approve <request-id>
```

默认选择 Agent 提供的 `allow_once` 选项。也可以明确指定 option ID：

```text
woodenox approve <request-id> <option-id>
```

拒绝：

```text
woodenox reject <request-id>
```

## 失败和重试

任务配置无效、Agent 子进程退出、ACP 连接中断或 Agent 以非正常原因停止时，任务会使用滴答原生的“已放弃”状态。任务不会自动重试。将已放弃任务重新标记为未完成后，woodenox 会重新执行任务。

Agent 正常结束后，最终回复会写入任务评论，然后任务被标记完成。已完成任务再次打开时，会优先恢复之前的 ACP session。
