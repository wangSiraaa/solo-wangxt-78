# webhookd — 可靠的 Webhook 投递服务

Go (Gin) + PostgreSQL 实现的业务事件推送服务。数据库是唯一事实来源：
事件写入、订阅匹配、待投递记录在**同一个事务**中落库（transactional
outbox），投递 worker 只从数据库领取任务，进程崩溃不丢事件。

## 架构

```
                ┌──────────────────────── PostgreSQL ────────────────────────┐
 POST /v1/events│  events  ─┐                                                 │
  (Idempotency-Key)         │  同一事务（outbox）                              │
        │                   ▼                                                 │
        ▼              deliveries (pending)  ←── 订阅匹配 (endpoints 表)       │
   httpapi (Gin)            │                                                 │
                            │  SELECT ... FOR UPDATE SKIP LOCKED              │
                            ▼                                                 │
                      dispatcher workers ── 签名 HTTP POST ──▶ 客户接收器      │
                            │                                                 │
                            └── delivery_attempts / dead → 重放 (同一事件 id) │
                └─────────────────────────────────────────────────────────────┘
```

## 关键语义

| 主题 | 行为 |
|---|---|
| 事件身份 | `events.id` 创建后不可变；重试、死信重放都用**同一个事件 id**（`Webhook-Id` 头），绝不制造新业务事实 |
| 发布幂等 | `Idempotency-Key` 唯一约束；重复发布返回原事件（200, `deduplicated: true`），不产生新投递 |
| 投递保证 | 至少一次。2xx 成功；**429** 重试并遵守 `Retry-After`；**5xx/超时/连接重置**（响应可能丢失）按指数退避重试；其余 4xx 与 3xx（不跟随重定向）直接进入死信 |
| 响应丢失 | 请求超时/连接中断时事件可能已被对方处理——重投使用同一事件 id，由接收器去重保证业务效果只执行一次 |
| 死信重放 | `POST /v1/deliveries/{id}/replay` 仅重排原 delivery，事件表不新增行；若代次已迁移则重放到当前代次 |
| 每端点策略 | 独立签名密钥（可轮换）与重试策略（最大次数/退避基数/上限/HTTP 超时） |

> **关于"恰好一次"**：网络投递无法保证端到端恰好一次——本服务保证的是
> **至少一次投递 + 稳定事件身份**，端到端恰好一次的业务效果由接收方幂等
> （事件 id 去重 + 业务键幂等）达成。

## 按业务键划分的投递序列

每个事件携带 `business_key` 与按键单调递增的 `seq`（发布事务内分配）：

* **同一业务键**（如同一订单）严格按 `seq` 顺序投递：前序事件未解决
  （成功或被跳过），后续事件不会被认领；
* **不同业务键**之间完全并行；
* 顺序约束按端点**谱系（lineage）**生效，跨代次迁移后依然连续。

下游按 `business_key` 跟踪 `seq` 即可发现**序列缺口**（被跳过的毒消息）。

## 端点代次与域名迁移

更换接收域名不是原地改 URL，而是代次迁移
（`POST /v1/endpoints/{id}/migrate`，单事务完成）：

| 事件状态 | 处理口径 |
|---|---|
| 迁移前**已处理**（succeeded） | 保持成功，不重投、不迁移 |
| **在途**（delivering） | 允许旧域名上的请求完成；其回执只确认**旧代次自己的** delivery 行——迟到成功不会确认新代次的任何任务。若该尝试失败，重试自动改指当前代次（新域名） |
| **未投递**（pending，含等待重试） | 迁移事务内直接改指新代次，后续在新域名投递 |
| 死信 | 保持死信；人工 `replay` 时投递到当前代次 |

旧代次行保留为审计轨迹（`status='superseded'`），`lineage_id` 贯穿所有代次。

## 毒消息与人工跳过

* 毒消息（耗尽重试进入 `dead`）**只阻塞其业务键**，其他键不受影响；
* 跳过是人工决定：`POST /v1/deliveries/{id}/skip` 必须携带 `reason`
  （可附 `resolved_by`），且只能跳过 `dead` 状态的投递；
* 跳过的事件**永不投递**，其 `seq` 成为下游可见的序列缺口；
  `GET /v1/deliveries?business_key=...` 可查看该键的完整序列状态。

## 密钥轮换（限定双密钥窗口）

`POST /v1/endpoints/{id}/rotate-secret` 接受 `window_seconds`
（默认 3600，最大 86400）：轮换后旧密钥仅在窗口期内可被接收方接受，
**不会无限保留**。投递方立即用新密钥签名；接收方应在窗口期内同时接受
新旧两个密钥（见 `signature.VerifyWithRotation` 与 `cmd/receiver` 的
`PREVIOUS_SECRET` / `PREVIOUS_SECRET_EXPIRES`）。

## 签名方案

```
Webhook-Id: <event uuid>
Webhook-Timestamp: <unix 秒>
Webhook-Signature: t=<unix>,v1=<hex>     # v1 = HMAC-SHA256(secret, "<t>.<raw-body>")
```

接收器必须：① 用端点密钥验签（常量时间比较）；② 拒绝超出 ±5 分钟时间窗的时间戳；
③ 以事件 id 去重。固定测试向量见
[`internal/signature/testdata/vectors.json`](internal/signature/testdata/vectors.json)
（重新生成：`make vectors`），任何语言都可据此校验互通性。

## 重复投递 ≠ 重复业务处理

接收器（`cmd/receiver` 与内置 testkit）维护两张表：

* `deliveries[event_id]` — 传输层计数。同一事件 id 再次到达 = **重复投递**，
  直接 ACK 不再处理；
* `business[business_key]` — 业务层效果。只有首次见到该事件 id 才执行，
  因此**业务效果恰好一次**，即使投递了多次。

`GET /testkit/state` 同时展示两个计数器，可直观对比。

## SSRF 防护

* 注册时：仅允许 http/https、禁止 userinfo；解析主机并拒绝环回（本机管理口
  所在）、RFC1918、链路本地（含云元数据 169.254.169.254）、CGNAT、组播与保留段；
* 投递时：自定义 DialContext 在连接前**重新解析并校验 IP**，防 DNS rebinding；
* **永不跟随重定向**（3xx 视为失败），已验证的地址无法把 worker 弹到内网；
* 本地测试接收器必须显式加入白名单：`ALLOW_PRIVATE_HOSTS=127.0.0.1/32`
  （生产环境保持为空）。

## 快速开始

```bash
# 1. 起 PostgreSQL（或用自己的实例）
docker compose -f deploy/docker-compose.yml up -d postgres

# 2. 启动服务（本地开发允许 127.0.0.1 接收器）
DATABASE_URL='postgres://postgres:postgres@localhost:5432/webhookd?sslmode=disable' \
ALLOW_PRIVATE_HOSTS='127.0.0.1/32' \
go run ./cmd/server

# 3. 注册端点（testkit 是内置的本地测试接收器，返回签名密钥）
curl -s localhost:8080/v1/endpoints -d '{
  "url": "http://127.0.0.1:8080/testkit/receive?secret=TEST_ONLY_WEBHOOK_DEMO_SECRET&key=demo",
  "secret": "TEST_ONLY_WEBHOOK_DEMO_SECRET",
  "subscribed_events": ["order.created"],
  "retry_policy": {"max_attempts": 5, "backoff_base_ms": 500}
}' | jq .

# 4. 发布事件（同事务写入事件+投递记录）
curl -s localhost:8080/v1/events \
  -H 'Idempotency-Key: order-42-created' \
  -d '{"type":"order.created","data":{"business_key":"order-42","amount":100}}' | jq .

# 5. 观察投递与接收器状态
curl -s 'localhost:8080/v1/deliveries?status=succeeded' | jq .
curl -s localhost:8080/testkit/state | jq .
```

### 失败模拟（testkit）

`POST /testkit/mode {"key":"demo","mode":"..."}` 切换行为：

| mode | 行为 |
|---|---|
| `ok` | 正常 200 |
| `fail500` | 恒 500（耗尽重试后进入死信） |
| `fail429` | 恒 429 + `Retry-After` |
| `flaky:N` / `flaky429:N` | 先失败 N 次再成功 |
| `timeout` / `timeout:N` | 先处理业务再超时响应（响应丢失场景） |
| `drop` / `drop:N` | 先处理业务再直接断连（响应丢失场景） |
| `redirect` | 返回 302（验证 dispatcher 不跟随重定向） |

### 死信重放

```bash
# 端点处于 fail500 时发布事件 → delivery 变 dead
curl -s -XPOST localhost:8080/v1/deliveries/<delivery-id>/replay | jq .
# 重放使用原事件 id；events 表行数不变
```

### 独立客户接收器示例

```bash
WEBHOOK_SECRET=TEST_ONLY_WEBHOOK_DEMO_SECRET PORT=9090 go run ./cmd/receiver
# POST /webhook 验签+时间窗+事件 id 去重；GET /state 查看计数器
```

## 测试

```bash
make test-unit         # 签名向量 / 重试退避 / SSRF 规则 / 双密钥轮换窗口
make test-integration  # 嵌入式 PostgreSQL 端到端：
                       #   429、5xx、响应丢失、幂等发布、重定向拒绝、死信重放身份不变
                       #   同键顺序与重试（[1,1,1,2,3]）、跨键并行（键内并发=1）
                       #   毒消息只阻塞本键、人工跳过带原因、下游可见序列缺口
                       #   迁移时在途请求完成（旧代次行）/在途失败新代次续投
                       #   轮换窗口内双密钥接受、窗口外旧密钥拒绝
```

集成测试通过 `embedded-postgres` 拉起真实数据库；唯一被允许的接收器是
显式白名单 `127.0.0.1/32` 下的本地 testkit。

## API

完整定义见 [`api/openapi.yaml`](api/openapi.yaml)。主要端点：

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/v1/endpoints` | 注册端点（SSRF 校验；密钥仅此一次返回） |
| GET/PATCH/DELETE | `/v1/endpoints/{id}` | 查询 / 更新（不可改 URL）/ 停用 |
| POST | `/v1/endpoints/{id}/migrate` | 域名迁移：新代次，旧代次 superseded |
| POST | `/v1/endpoints/{id}/rotate-secret` | 轮换签名密钥（`window_seconds` 限定双密钥窗口） |
| POST | `/v1/events` | 发布事件（需 `Idempotency-Key` 头；支持 `business_key`） |
| GET | `/v1/events/{id}` | 事件及其投递记录 |
| GET | `/v1/deliveries` | 按状态/事件/端点/业务键过滤 |
| GET | `/v1/deliveries/{id}` | 投递详情 + 尝试日志 |
| POST | `/v1/deliveries/{id}/replay` | 死信重放（保留原事件，走当前代次） |
| POST | `/v1/deliveries/{id}/skip` | 人工跳过毒消息（必须带 `reason`） |

## 配置（环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `DATABASE_URL` | `postgres://postgres:postgres@localhost:5432/webhookd?sslmode=disable` | PG 连接串 |
| `HTTP_ADDR` | `:8080` | API 监听地址 |
| `DELIVERY_WORKERS` | `4` | 投递并发数 |
| `POLL_INTERVAL_MS` | `500` | 投递轮询间隔 |
| `ALLOW_PRIVATE_HOSTS` | 空 | 私网白名单（CIDR/主机名，逗号分隔），**仅测试用** |
| `ENABLE_TESTKIT` | `true` | 是否挂载 `/testkit` 失败模拟接收器，生产应设 `false` |
