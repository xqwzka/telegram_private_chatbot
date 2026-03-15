# Telegram Private Chatbot（Cloudflare Workers 版）

这是一个基于 Cloudflare Workers 的 Telegram 私聊转发机器人。

当前版本功能定位：
- 用户私聊机器人后，先做人机验证（题库 + 按钮选项）。
- 验证通过后，用户消息自动转发给管理员账号（不是群组）。
- 管理员回复转发消息，可直接回给对应用户。
- 提供封禁、解封、关闭会话、恢复会话、重置验证等管理命令。

## 当前版本与旧版差异

当前代码（`worker.js`）已改为“管理员 UID 模式”，不再依赖群组 Topic。

必须变量变更如下：
- 旧版常见：`SUPERGROUP_ID`
- 当前版本：`ADMIN_UID`

## 功能说明

- 人机验证：
  - 使用本地题库（`LOCAL_QUESTIONS`）。
  - 用户点击按钮作答，无需手动输入答案。
  - 验证有效期：5 分钟（`VERIFY_TTL_SECONDS`）。
  - 验证通过缓存：30 天（`VERIFIED_TTL_SECONDS`）。
- 消息转发：
  - 用户消息 -> 转发到管理员私聊。
  - 管理员在“转发来的那条消息下面直接回复”，机器人会回传给对应用户。
- 安全控制：
  - 拦截普通用户发送的斜杠命令（除 `/start`）。
  - 支持封禁/解封、会话关闭/恢复、验证重置、状态查询。

## 管理员命令

管理员可在自己的私聊中使用这些命令：

- `/ban <uid>`：封禁用户（机器人完全忽略该用户消息）
- `/unban <uid>`：解封用户
- `/close <uid>`：关闭该用户会话（用户会收到“会话已关闭”提示）
- `/open <uid>`：恢复该用户会话
- `/reset <uid>`：清空该用户验证状态，下次需重新验证
- `/info <uid>`：查看用户状态（Verified/Banned/Closed）

说明：
- 也可以不写 `<uid>`，直接“回复管理员侧那条转发消息”后发命令，机器人会自动识别目标用户。

## 部署前准备

1. 创建 Telegram Bot
   - 在 [@BotFather](https://t.me/BotFather) 创建机器人，拿到 `BOT_TOKEN`。

2. 获取管理员 UID（你的 Telegram 账号数字 ID）
   - 可使用 `@userinfobot` 或类似机器人查询。
   - 记下纯数字 UID，后面填到 `ADMIN_UID`。

3. Cloudflare 账号
   - 开通 Workers。
   - 创建一个 KV Namespace（名称建议 `TOPIC_MAP`，与代码绑定名保持一致）。

## Cloudflare Dashboard 手动部署（推荐新手）

1. 进入 Cloudflare -> Workers 和 Pages -> 创建 Worker。
2. 点击编辑代码，把仓库里的 `worker.js` 全量覆盖进去并部署。
3. 打开该 Worker 的设置 -> 变量和机密，添加：
   - 文本变量：`BOT_TOKEN` = 你的 Telegram Bot Token
   - 文本变量：`ADMIN_UID` = 你的管理员 UID（纯数字）
4. 在同一页面添加 KV 绑定：
   - 绑定变量名：`TOPIC_MAP`
   - 选择你创建的 KV Namespace
5. 保存并重新部署。

## 使用 Wrangler 部署（命令行）

确保已安装 Node.js 与 Wrangler：

```bash
npm install -g wrangler
wrangler login
```

初始化 KV（仅首次）：

```bash
wrangler kv namespace create TOPIC_MAP
```

将返回的 `id` 填入 `wrangler.toml` 的 KV 绑定。

设置机密：

```bash
wrangler secret put BOT_TOKEN
wrangler secret put ADMIN_UID
```

部署：

```bash
wrangler deploy
```

## 设置 Telegram Webhook（必须）

部署完成后，拿到你的 Worker URL（例如 `https://telegram.xxx.workers.dev`），执行：

```text
https://api.telegram.org/bot<你的BOT_TOKEN>/setWebhook?url=<你的Worker完整URL>
```

示例：

```text
https://api.telegram.org/bot123456:ABCDEF/setWebhook?url=https://telegram.xxx.workers.dev
```

返回：

```json
{"ok":true,"result":true,"description":"Webhook was set"}
```

表示设置成功。

## 快速自检

1. 普通用户给机器人发消息。
2. 机器人应先发送验证题按钮。
3. 选对后再发消息，管理员应能收到转发。
4. 管理员直接回复那条转发消息，用户应能收到回复。

## 常见问题

1. 设置了 Webhook 但没反应
   - 检查 Worker 是否是最新部署版本。
   - 检查变量名是否完全一致：`BOT_TOKEN`、`ADMIN_UID`、`TOPIC_MAP`。
   - 重新设置一次 webhook（必要时先 `deleteWebhook` 再 `setWebhook`）。

2. 管理员回复后用户收不到
   - 必须回复“机器人转发来的那条消息”。
   - 若你不是 `ADMIN_UID` 对应账号，机器人会把你当普通用户。

3. 验证一直重复
   - 检查 KV 绑定是否正确，`TOPIC_MAP` 是否真的绑定到有效 Namespace。

## 代码结构

- `worker.js`：主逻辑（当前生效版本）
- `wrangler.toml`：Workers 部署配置
- `ref_worker.js` / `worker.raw.js`：历史或参考版本（不一定是当前生产逻辑）

## 免责声明

请妥善保管 `BOT_TOKEN` 和 Cloudflare 凭据，不要提交到公开仓库。
