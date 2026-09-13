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
| 死信重放 | `POST /v1/deliveries/{id}/replay` 仅重排原 delivery，事件表不新增行 |
| 每端点策略 | 独立签名密钥（可轮换）与重试策略（最大次数/退避基数/上限/HTTP 超时） |

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
make test-unit         # 签名向量 / 重试退避 / SSRF 规则
make test-integration  # 嵌入式 PostgreSQL 端到端：429、5xx、响应丢失、
                       # 幂等发布、重定向拒绝、死信重放身份不变
```

集成测试通过 `embedded-postgres` 拉起真实数据库；唯一被允许的接收器是
显式白名单 `127.0.0.1/32` 下的本地 testkit。

## API

完整定义见 [`api/openapi.yaml`](api/openapi.yaml)。主要端点：

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/v1/endpoints` | 注册端点（SSRF 校验；密钥仅此一次返回） |
| GET/PATCH/DELETE | `/v1/endpoints/{id}` | 查询 / 更新 / 停用 |
| POST | `/v1/endpoints/{id}/rotate-secret` | 轮换签名密钥 |
| POST | `/v1/events` | 发布事件（需 `Idempotency-Key` 头） |
| GET | `/v1/events/{id}` | 事件及其投递记录 |
| GET | `/v1/deliveries` | 按状态/事件/端点过滤 |
| GET | `/v1/deliveries/{id}` | 投递详情 + 尝试日志 |
| POST | `/v1/deliveries/{id}/replay` | 死信重放（保留原事件） |

## 配置（环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `DATABASE_URL` | `postgres://postgres:postgres@localhost:5432/webhookd?sslmode=disable` | PG 连接串 |
| `HTTP_ADDR` | `:8080` | API 监听地址 |
| `DELIVERY_WORKERS` | `4` | 投递并发数 |
| `POLL_INTERVAL_MS` | `500` | 投递轮询间隔 |
| `ALLOW_PRIVATE_HOSTS` | 空 | 私网白名单（CIDR/主机名，逗号分隔），**仅测试用** |
| `ENABLE_TESTKIT` | `true` | 是否挂载 `/testkit` 失败模拟接收器，生产应设 `false` |
