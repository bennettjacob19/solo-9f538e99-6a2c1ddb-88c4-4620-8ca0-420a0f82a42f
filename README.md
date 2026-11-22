# Kubernetes ServiceAccount 令牌轮换 API

基于 **FastAPI + SQLite** 的服务账号令牌轮换服务，供值班员在 **Pod 持续运行**
期间更换凭据。核心保证：

- 发起轮换时立即签发新一代令牌，旧代进入 **宽限期（superseded）**，期内仍可验证；
- 只有 **所有存活 Pod 确认已装载新代令牌** 后才撤销旧代；
- 宽限期到期后旧代 **立即拒绝**（验证时惰性失效 + 后台协程定时清扫）；
- 轮换、Pod 重启、确认并发执行时，**过期/旧代确认会被拒绝，绝不可能撤销更新后的令牌**；
- Pod 故障或重启后 **沿用上次已提交代数** 的令牌，需要对未完成轮换重新确认。

交互式文档（Swagger UI）：启动后访问 <http://localhost:8000/docs>。

---

## 1. 启动方式

### docker compose（推荐）

```bash
docker compose up --build
# API:  http://localhost:8000
# 文档: http://localhost:8000/docs
```

SQLite 数据文件持久化在命名卷 `token-rotation-data`（容器内 `/data/tokens.db`）。
可通过环境变量调整：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `TOKEN_ROTATION_DB` | `/data/tokens.db` | SQLite 数据库路径 |
| `SWEEP_INTERVAL_SECONDS` | `1` | 后台清扫宽限期过期令牌的间隔（秒） |

### 本地直接运行

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
TOKEN_ROTATION_DB=./data/tokens.db .venv/bin/uvicorn app.main:app --reload
```

### 运行自测

```bash
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m pytest tests/ -v
```

---

## 2. 数据模型与令牌代数

- `service_accounts`：服务账号，登记时自动签发 **第 1 代** 令牌。
- `token_generations`：令牌代数，状态机为
  `active → superseded（宽限期内仍可验证）→ revoked`。
- `pods`：使用该服务账号的 Pod，记录 `last_committed_gen`（上次已成功提交的代数）。
- `rotations`：一次轮换，状态为
  `pending → completed / expired / superseded`。
- `confirmations`：Pod 对某轮换的确认记录。

令牌格式为 `k8s-sat-<32 字节随机串>`，仅以明文存于本服务的 SQLite 中
（模拟 K8s 项目化服务，生产中应配合 KMS/密文存储）。

---

## 3. API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/service-accounts` | 登记服务账号（自动签发第 1 代令牌） |
| GET | `/service-accounts` | 列出服务账号 |
| GET | `/service-accounts/{id}` | 详情：各代令牌 + Pod 列表 |
| POST | `/service-accounts/{id}/pods` | 登记使用该 SA 的 Pod（默认装载当前代） |
| POST | `/pods/{id}/restart` | 模拟重启/故障恢复，沿用上次已提交代数 |
| POST | `/pods/{id}/alive?alive=true|false` | 标记存活/下线 |
| POST | `/service-accounts/{id}/rotations` | 发起轮换（body 指定 `grace_seconds`） |
| GET | `/rotations?sa_id=` | 查询轮换列表/状态 |
| GET | `/rotations/{id}` | 轮换详情：已确认/待确认 Pod、旧代到期时间 |
| POST | `/rotations/{id}/confirm?pod_id=` | Pod 确认已装载新代（body 带 `generation`） |
| POST | `/verify` | 验证令牌（body 带 `token`） |

---

## 4. 典型操作流程（curl）

```bash
# 1) 登记服务账号（自动得到第 1 代令牌）
curl -s -X POST localhost:8000/service-accounts \
  -H 'Content-Type: application/json' \
  -d '{"name":"metrics-reader","namespace":"monitoring"}'

# 2) 登记两个正在运行的 Pod
curl -s -X POST localhost:8000/service-accounts/1/pods \
  -H 'Content-Type: application/json' -d '{"name":"pod-a","namespace":"monitoring"}'
curl -s -X POST localhost:8000/service-accounts/1/pods \
  -H 'Content-Type: application/json' -d '{"name":"pod-b","namespace":"monitoring"}'

# 3) 发起轮换：宽限 300 秒，旧代变 superseded、新代 active
curl -s -X POST localhost:8000/service-accounts/1/rotations \
  -H 'Content-Type: application/json' -d '{"grace_seconds":300}'

# 4) 宽限期内旧令牌仍可验证
curl -s -X POST localhost:8000/verify \
  -H 'Content-Type: application/json' -d '{"token":"<旧代令牌>"}'
# -> {"valid": true, "reason": "valid (superseded generation within grace period)", ...}

# 5) Pod 热加载新令牌后逐个确认（pod_id 用查询参数，代数放 body）
curl -s -X POST 'localhost:8000/rotations/1/confirm?pod_id=1' \
  -H 'Content-Type: application/json' -d '{"generation":2}'
curl -s -X POST 'localhost:8000/rotations/1/confirm?pod_id=2' \
  -H 'Content-Type: application/json' -d '{"generation":2}'
# 第二个确认返回 status=completed，旧代立即 revoked

# 6) 旧令牌此后验证被拒
curl -s -X POST localhost:8000/verify \
  -H 'Content-Type: application/json' -d '{"token":"<旧代令牌>"}'
# -> {"valid": false, "reason": "token generation has been revoked", ...}
```

### 故障与重启

```bash
# Pod 崩溃/重启：返回它继续使用的代数与令牌，并提示待确认的轮换
curl -s -X POST localhost:8000/pods/1/restart
# -> {"reused_generation": 1, "token": "...", "needs_confirmation_for_rotation": 1}
# 该 Pod 在轮换状态中重新变为 pending，需要再次 confirm
```

### 过期确认的防护

- 对已 `completed / expired / superseded` 的轮换再确认 → `409 late confirmation rejected`；
- 上报旧代数（如轮换要 2、Pod 报 1）→ `409 stale generation ...`；
- 上报代数小于 Pod 已提交代数 → `409 ... stale confirmation rejected`；
- 以上拒绝 **不产生任何状态变更**，因此永远不会误撤销新一代令牌。

---

## 5. 并发与一致性设计

1. 所有写操作获取同一把进程内 `threading.RLock`，并在 `BEGIN IMMEDIATE`
   事务中完成“读取校验 + 写入”，SQLite 开启 WAL，轮换/确认/重启被完全串行化。
2. 撤销旧代的 SQL 带 `status != 'active'` 保护，且只在
   “全部存活 Pod 均已确认”的事务内执行。
3. 并发发起多次轮换时，较早的 pending 轮换被标记为 `superseded`，
   只有最新一次可完成；任意时刻只有一代 `active`。
4. 宽限期到期有两条收敛路径：`/verify` 惰性失效 + 后台 sweeper 定时清扫，
   到期后对应未完成轮换置为 `expired`。
5. 重启只清除该 Pod 的确认记录、不回退 `last_committed_gen`；
   已完成轮换的撤销不会因后续重启而撤销。

边界约定：服务账号下没有任何存活 Pod 时发起轮换，因无需任何 Pod 装载新代，
轮换立即完成并撤销旧代；之后新登记的 Pod 直接装载当前最新代。

---

## 6. 自测覆盖（29 个用例，全部通过）

| 测试文件 | 覆盖内容 |
| --- | --- |
| `test_registration.py` | SA/Pod 登记、重复冲突、令牌验证、`/docs` 可用 |
| `test_rotation.py` | 轮换状态机、全部存活 Pod 确认后才撤销、下线 Pod 不挡法定人数、外 Pod 拒绝 |
| `test_grace_period.py` | 宽限期内有效、到期立即拒绝、后台 sweeper、真实时间端到端 |
| `test_stale_and_recovery.py` | 旧代/迟到确认拒绝且不改状态、连续轮换、重启沿用代数并重新确认、过期后恢复 |
| `test_concurrency.py` | 多线程同时确认、轮换×重启×确认交错、并发轮换串行化、重启抖动下最终一致 |
| `test_edge_cases.py` | 无 Pod 轮换立即完成、后登记 Pod 装载最新代、Pod 复活后重新确认 |

另做过一次真实 uvicorn 进程的端到端冒烟（登记→轮换→宽限验证→拒绝旧代确认→
重启→全部确认→撤销→宽限到期清扫），行为全部符合预期。

---

## 7. 未完成 / 简化项

- **未对接真实 Kubernetes API**：Pod 登记、重启、令牌热加载均为 API 内模拟，
  生产化需替换为 K8s informer/投影卷（bound service account token）机制。
- **无认证鉴权**：接口默认信任调用方（值班内网场景）；如需对外开放应加
  API Key / OIDC 网关。
- **单进程部署假设**：并发安全依赖进程内锁与单 SQLite 文件；多副本水平扩展需
  换用带行锁/事务的数据库或分布式锁。
- 令牌明文存储，未集成 KMS/信封加密；无令牌轮换审计日志导出接口。
