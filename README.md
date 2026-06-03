# 🐕 Hermes 看门狗

> 🔍 检测 Hermes Agent 飞书 WebSocket 静默断开并自动恢复

---

## 🧊 要解决的问题

`hermes gateway status` 显示 `active (running)`，但你的 Bot 已经几个小时没反应了。这是 **TCP 半开连接** —— 飞书服务端已断开，但操作系统从未通知 gateway 进程。网关看起来"活着"，实则收不到任何消息。

```
🟢 systemd: 进程正在运行
💬 飞书服务器: WebSocket 已断开
🤖 你的 Bot: 假装活着 → 实则已死

看门狗的作用: 检测这种"假活"并自动重启 🩺
```

---

## ⚙️ 工作原理

不是检查进程是否存活（没用，通常都活着），而是交叉比对两个数据源：

| 📡 数据源 | 🔍 检查方式 |
|----------|------------|
| **飞书 REST API** | 拉取用户最近一条发给 Bot 的消息 |
| **Gateway 日志** | 检查该消息 ID 是否到达了 gateway |

```
飞书上有这条消息  +  日志里没这条消息  =  WebSocket 已死  →  自动重启
```

---

## 🚀 快速开始

```bash
# 📋 自动发现所有 hermes-gateway-* 服务
python3 hermes-watchdog.py

# 🎯 仅监控单个 profile
python3 hermes-watchdog.py --profile english

# 🧪 干跑模式：仅检测，不重启
python3 hermes-watchdog.py --no-restart

# ⏰ 通过 cron 定时执行（每 10 分钟）
*/10 * * * * /usr/bin/python3 /path/to/hermes-watchdog.py
```

---

## 🌍 跨机器移植

所有路径均可配置。脚本通过环境变量自动检测，并配有合理的默认值：

| 📌 环境变量 | 📂 默认值 | 🎯 用途 |
|------------|----------|--------|
| `HERMES_HOME` | `~/.hermes` | Hermes 根目录 |
| `WATCHDOG_LOG_DIR` | `$HERMES_HOME/watchdog` | 看门狗日志输出 |
| `WATCHDOG_STATE_DIR` | 同日志目录 | 每个 profile 的状态文件 |

在其他机器上运行 —— 只需设置 `HERMES_HOME`：

```bash
HERMES_HOME=/opt/hermes python3 hermes-watchdog.py --profile default
```

---

## 🔌 适配 OpenClaw 或其他框架

看门狗不局限于 Hermes。覆盖服务匹配模式、重启命令和日志路径即可适配：

```bash
# 🦀 OpenClaw 示例
python3 hermes-watchdog.py \
  --profiles-root /root/.openclaw/profiles \
  --service-pattern 'openclaw-gateway-(\w+)' \
  --restart-cmd 'openclaw gateway restart --profile {profile}' \
  --gateway-log '{profiles_root}/{profile}/logs/gateway.log'
```

> ⚠️ **非 Hermes 网关注意事项**：检测依赖于飞书消息 ID（`om_*`）出现在 gateway 日志中。如果你的网关不记录消息 ID，需要换一种检测策略（如时间戳对比）。

---

## 📖 CLI 参数参考

```
用法: hermes-watchdog.py [选项]

  --profile NAME          🎯 监控单个 profile（默认：自动发现全部）
  --profiles-root PATH    📂 Profiles 目录（默认：~/.hermes/profiles）
  --service-pattern RE    🔍 systemd 单元正则（默认：hermes-gateway-(\w+)）
  --restart-cmd CMD       🔄 重启命令，支持 {profile} 占位符
  --gateway-log PATH      📝 Gateway 日志路径，支持 {profiles_root} 和 {profile}
  --no-restart            🧪 仅检测，不重启
  --interval N            ⏱️ 两次重启最小间隔（分钟，默认：15）
  --missed-threshold N    ⏳ 判定死亡前的等待时间（分钟，默认：5）
  --max-age N             📅 消息最大年龄，超时视为空闲（分钟，默认：120）
  --log-dir PATH          📁 看门狗日志目录
  --state-dir PATH        💾 状态文件目录
```

---

## 🔢 退出码

| 码值 | 含义 | 📬 Cron 动作建议 |
|------|------|-----------------|
| `0` | ✅ 全部健康或空闲 | 静默 |
| `1` | 🔴 检测到死 WebSocket，已重启 | 可通知 |
| `2` | ❌ 看门狗自身失败（凭证/API/网络） | **告警！** |

> 💡 设计用于 `no_agent=true` 的 cron 模式 —— 每次检查零 token 消耗。

---

## 🧬 详细工作流程

```
1. 📡 扫描 systemd --user 中的 hermes-gateway-*.service（或 --profile 指定）
        │
2. 🔍 对每个运行中的 gateway：
   ├── a. 从 profile .env 读取 FEISHU_APP_ID/SECRET
   ├── b. 获取 tenant_access_token
   ├── c. 解析 chat_id（4 级降级策略）：
   │      .env → 状态缓存 → IM API 列表 → 日志 grep
   ├── d. 通过飞书 IM API 拉取最新用户消息
   ├── e. 在 gateway 日志中 grep 消息 ID
   ├── f. 消息存在但日志中没有超过 5 分钟 → 🔴 DEAD → 重启
   └── g. 消息超过 120 分钟 → 🟢 空闲（日志可能已轮转）
        │
3. ⏱️ 冷却保护：每个 profile 每 15 分钟最多重启一次
        │
4. 🏁 返回所有 profile 中最严重的退出码
```

---

## 🛡️ 防误报机制（v2）

| 🛡️ 保护措施 | 📖 说明 |
|------------|------|
| 🔄 **重启后保护** | 如果最新消息的时间戳早于上次重启时间，不会误判为死亡 |
| 📅 **日志轮转保护** | 超过 120 分钟的消息视为日志已被轮转，不会误报 |
| ⏳ **宽限期** | 消息在 5 分钟内未到达日志不报警（考虑网络延迟） |
| 🆒 **冷却机制** | 15 分钟内不重复重启同一 gateway |
| 🔁 **API 重试** | 飞书 API 调用失败自动指数退避重试（最多 2 次） |

---

## 📋 环境要求

| 🧩 组件 | 📌 要求 |
|--------|--------|
| 🐍 **Python** | 3.10+ |
| 🐧 **操作系统** | Linux + systemd user service |
| 🤖 **飞书 Bot** | 拥有 `im:message:readonly` 权限 |
| 🔐 **认证密钥** | 每个 profile 的 `.env` 中配置 `FEISHU_APP_ID` 和 `FEISHU_APP_SECRET` |

---

## 📜 开源协议

[MIT](https://opensource.org/licenses/MIT)

---

> 🐕 *一个静默运行、从不误报、出问题秒恢复的看门狗。*
