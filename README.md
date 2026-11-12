# LIMS 生化仪接口接收服务（HL7 v2.5.1 ORU^R01 / MLLP + 结果标准化）

单进程接收服务：FastAPI + SQLite，MLLP TCP 端口只接收单台生化仪的 ASCII
HL7 v2.5.1 `ORU^R01`。每条消息一个 OBR、1～20 个不重复的数值 OBX；
样本号取 OBR-3.1，项目取 OBX-3.1，值取 OBX-5，状态取 OBX-11。

仪器结果同时按**标准化映射**换算为统一项目/单位，使用 `decimal.Decimal`
精确十进制乘法（绝不使用 float），换算结果预先落库、按样本直接查询，
不在查询时临时换算。映射最多 20 行、整表替换；替换在后台重算，旧版本
继续提供查询，新旧版本随映射与全部结果在一个事务里原子切换。

## 快速启动（Docker Compose）

```bash
docker compose up --build
```

| 入口 | 地址 |
|---|---|
| 演示 / 交互式 API 文档 | http://localhost:8000/docs |
| 按样本查询当前结果 | http://localhost:8000/api/v1/samples/{样本号} |
| 按 MSH-10 查询消息与原回执 | http://localhost:8000/api/v1/messages/{MSH-10} |
| 样本列表 | http://localhost:8000/api/v1/samples |
| 健康检查 | http://localhost:8000/health |
| 仪器 MLLP 端口 | `localhost:2575`（VT…FS+CR，每帧一个 ACK） |

数据保存在命名卷 `lis-data` 的 `/data/lis.db`（WAL、`synchronous=FULL`），
容器重启后去重记录与结果都保留。（无 Docker 时用 `./run_local.sh` 启动同一
服务，见下文“本地开发与测试”。）

### 在 /docs 中演示

`POST /api/v1/demo/messages` 不是直接写库，而是把消息通过**真实 MLLP
socket** 发送到本服务，可配置：

* `fragments`：把整帧拆成 N 次 TCP write（演示分片）；
* `delay_ms`：每次 write 之间停顿；
* `drop_ack`：第一次发送后丢弃回执并立即重发完全相同的字节（演示
  "提交后丢回执重发"），响应里会有两条 ACK，可直接比对是否逐字节相同。

示例消息（换行可用 `\n`，服务会规范成 CR）：

```
MSH|^~\&|ANALYSER|LAB|LIS|HOSP|20261112090000||ORU^R01|DEMO-001|P|2.5.1
OBR|1|S01|S-800^LAB|||||||||||||DR
OBX|1|NM|GLU^x||5.0|mmol/L|||||F
OBX|2|NM|K^y||4.1|mmol/L|||||F
```

然后 `GET /api/v1/samples/S-800` 查看当前结果；把 `DEMO-001` 原样重发（或
`drop_ack=true`）会返回同一条已持久化的 AA，且结果不会被二次应用。

### 并发同 MSH-10

`POST /api/v1/demo/concurrent`（见 `/docs` 的 demo 分组）会用 N 条独立
连接同时发送同一条消息；也可用本地模拟器：

```bash
docker compose exec lis python -m app.simulator --host 127.0.0.1 --port 2575
```

模拟器依次演示例行：分片（40 段）、粘包（一连接连发 3 条）、丢 ACK 重发、
8 连接并发同 ID、F/C 修正、含非法项的整条拒绝。也可只跑某一个：

```bash
docker compose exec lis python -m app.simulator --host 127.0.0.1 --port 2575 \
    --scenario concurrent --scenario dropped-ack
```

## 结果标准化（统一项目/单位入仓）

### 数据模型与精确计算

每条映射含：源项目 `source_item`、预期源单位 `source_unit`、标准项目
`standard_item`、标准单位 `standard_unit`、**正十进制系数字符串**
`coefficient`（如 `"0.0113"`、`"1E-3"`；零、负数、NaN、Infinity 均拒绝）。
一次映射最多 20 行，`source_item` 与 `standard_item` 各自唯一。

标准值用 `Decimal(原值) * Decimal(系数)` 在固定 60 位精度上下文里精确
计算，输出纯十进制字符串（不使用科学计数法，不引入 float 误差，例如
`0.1 × 0.1 = 0.01`、`92 × 0.0113 = 1.0396`）。

不可用的结果**不会丢消息**，而是随标准行落库并给出原因，原始报文照常
接收、照常回 AA：

| reason | 含义 |
|---|---|
| `no_mapping` | 当前映射中没有该源项目 |
| `unit_mismatch` | 存在映射，但结果单位与映射的预期源单位不一致 |

### 整表替换、后台重算与原子切换

`PUT /api/v1/standard/mapping`（请求体见 `/docs`）执行整表替换：

1. 新映射作为一个 **pending 版本**与重算任务立即落库；旧 active 版本
   继续服务所有查询，期间 MLLP 的 F/C 照常接收。
2. 后台线程对全部当前结果重算标准行，写入新版本的影子区。换算与
   `sleep` 均在数据库写锁之外，只以短事务批量落库，不阻塞仪器接收
   （实测重算进行中的 MLLP 提交耗时与平时相同，约个位数毫秒）。
3. 重算期间提交的**新增（F）与更正（C）**由正常接收事务同时写入
   active 与 pending 版本；后台扫描对已存在的影子行用 `INSERT OR IGNORE`
   让位，**旧扫描永远不会覆盖更新的值**。
4. 切换在一个写事务内完成：补齐新版本仍缺失的行（重算后到切换前到达的
   结果）、翻转 active 指针、旧版本置 retired。映射与它的全部标准结果
   一起生效，要么全成要么全不成。
5. 一次只重算一份：已有任务运行时再次替换返回 **409**。重算失败或中断
   保留旧版本；进程重启时未完成的 `running` 任务标记为 `interrupted`，
   可重新发起替换。标准化只写 `standard_results` / 映射表，**不改写**
   原始结果（`results`）、原始报文与已存回执（`messages`）。

正常接收时，原始结果、标准结果、回执在**同一个 `BEGIN IMMEDIATE`
事务**里提交：一起成功或一起回滚。

### 标准化 API（均在 /docs 可见）

| 方法/路径 | 说明 |
|---|---|
| `GET /api/v1/standard/mapping` | 当前 active 映射版本及全部条目 |
| `PUT /api/v1/standard/mapping` | 整表替换（1–20 行），202 受理并后台重算；忙时 409；入参非法 422 |
| `GET /api/v1/standard/tasks` | 重算任务列表（状态/进度/错误/延迟） |
| `GET /api/v1/standard/tasks/latest` | 最近一次任务 |
| `GET /api/v1/standard/tasks/{task_id}` | 单任务状态（running/completed/failed/interrupted） |
| `GET /api/v1/standard/samples/{sample}` | 该样本的**预计算**标准结果（含 available 与 reason） |

重算延迟可调：请求体 `delay_ms`（单次替换生效，0–10000ms），或环境变量
`LIS_RECALC_DELAY_MS`（默认值）。延迟只作用于后台重算、发生在所有锁外。

### 重算期间更正样例（端到端脚本）

```bash
# 服务启动后（另开终端）；30ms/行让重算窗口可观察
docker compose exec lis python -m app.simulator \
    --host 127.0.0.1 --port 2575 \
    --scenario standardised --http-url http://127.0.0.1:8000 --delay-ms 30
```

脚本会：先收一条含 GLU/CREA/K 的 F（此时全部 `no_mapping`）→ 提交新映射
（演示并发替换返回 409）→ **在重算进行中**发 GLU 的 C 更正与新样本 F →
轮询任务至 completed → 打印标准结果，可看到更正后的值（`5.80` 而非
旧值 `7.10`）、精确换算（`92 umol/L → 1.0396 mg/dL`）以及无映射项的
`no_mapping`，并确认原始结果、报文与 AA 回执均未改变。

## 行为约定

### 接收与 ACK

* 每连接独立帧状态机，支持 TCP 分片、粘包、多帧连发、并发连接；
  VT…FS 之间为一条消息，帧外字节（如多余 CR）忽略。非 ASCII、NUL、
  空帧、超长（>256 KiB）帧回 AE。
* 解析/校验失败**整条拒绝**并回 AE，不写任何结果与消息记录。校验包括：
  MSH 必须首段且 `^~\&`；MSH-9 必须 `ORU^R01`；MSH-12 必须 `2.5.1`；
  MSH-10 必须 1–64 字符不含分隔符；OBR 恰好一条且 OBR-3.1 非空；1–20 个
  OBX；OBX-1 正整数；OBX-2 为 NM/SN 或空；OBX-3.1 非空且消息内唯一；
  OBX-5 必须有值且为数值（支持正负号/小数/科学计数）；OBX-11 必须 F 或 C；
  未知段、第二条 MSH/OBR 均拒绝。
* ACK：`MSH|^~\&|…|ACK^R01^ACK|…`，`MSA|AA|原 MSH-10|…`；AE 时附
  `ERR|段^错误码^HL70357||E`。仅当消息与回执在**同一事务提交成功之后**才
  发送 AA（`synchronous=FULL`，提交即落盘）。

### F / C 语义（与结果同事务）

* `F`：样本下**新增**项目；项目已存在则整条 AE（HL7 205）。
* `C`：**更正**样本下已有项目；项目不存在则整条 AE（HL7 207）。
* 一条消息内任一项目不满足上述规则，整条回滚、AE，其它项目也不会写入。

### MSH-10 去重与并发

* 已接纳的 MSH-10：
  * 原文逐字节相同（CR/LF 规范后）→ 返回当时持久化的**原 ACK**（MSA-2
    相同），不再应用；
  * 原文不同 → AE（HL7 205），不改动已有结果。
* 唯一性由 SQLite 主键 + `BEGIN IMMEDIATE` 写事务保证：同 ID 并发到达时
  只有一个事务应用，其余读到已提交记录并回放同一条 ACK；进程重启后同样
  成立（记录已持久化）。
* 提交前中断（进程崩溃/连接断开）由事务回滚保证不留部分结果；仪器重发后
  正常应用。未收到 FS 的不完整帧不触发任何写入。

### HTTP API

| 方法/路径 | 说明 |
|---|---|
| `GET /api/v1/samples/{sample}` | 当前原始结果（项目、值、单位、状态、来源 MSH-10、更新时间）及该样本已接纳消息列表 |
| `GET /api/v1/samples?limit=` | 最近更新的样本及项目数 |
| `GET /api/v1/messages/{msh10}` | 消息原文、原文摘要、ACK 代码、持久化的 ACK 原文 |
| `GET /api/v1/standard/...` | 标准化映射替换、重算任务、标准结果查询（见上文“结果标准化”） |
| `POST /api/v1/demo/messages` | 经真实 MLLP 发送（分片 / 丢回执重发） |
| `POST /api/v1/demo/concurrent` | N 连接并发发送同一条消息 |
| `GET /api/v1/demo/samples/{sample}` | 纯文本结果摘要 |

## 本地开发与测试

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
pytest                      # 解析、拒绝、F/C、并发、重启、分片/粘包、HTTP、demo、
                            # 精确十进制换算、映射版本/后台重算/原子切换
./run_local.sh              # http://localhost:8000/docs，MLLP 2575，库在 ./data/lis.db
python -m app.simulator --host 127.0.0.1 --port 2575
python -m app.simulator --scenario standardised --delay-ms 30
```

环境变量：`LIS_DB_PATH`（默认 `/data/lis.db`）、`LIS_MLLP_HOST`（默认
`0.0.0.0`）、`LIS_MLLP_PORT`（默认 `2575`）、`LIS_RECALC_DELAY_MS`
（后台重算每行延迟毫秒数，默认 `0`，可被单次请求的 `delay_ms` 覆盖）。

## 设计取舍

* **单进程 + 单写者**：接收进程只有一个 asyncio 循环，写操作经
  `asyncio.to_thread` 交给 SQLite 单写事务，配合进程内锁与
  `busy_timeout` 串行化；不引入外部队列，保证"提交后才回 AA"。
* **回执持久化**：把生成的 ACK 字节与结果一起存库，重发时原样返回，
  避免重新生成 ACK 导致时间戳/MSH-10 不同而破坏"同 ID 同回执"。
* **指纹用规范化原文**：CR/LF 差异不算"异文"，其余任何字节差异都算。
* **数值校验从严**：OBX-2 非 NM/SN、OBX-5 非数值都整条拒绝，避免 LIS
  里出现无法计算的结果。
* **标准化用版本化影子表，而非查询时换算**：`standard_results` 带
  `version_id`，查询只读 active 版本的预计算行。重算在 pending 版本的
  影子区进行，旧版本全程可查；切换是单个短事务（补齐缺口 + 翻转指针 +
  旧版本退役），保证映射与全部结果同时生效。
* **换算不持写锁、旧扫描不覆盖新值**：重算的 Decimal 计算与演示延迟都
  在锁外完成，仅以批量短事务落库且用 `INSERT OR IGNORE` 让位给接收
  事务（接收用 `INSERT OR REPLACE`，代表最新原始状态）；切换事务再做
  一次缺口补齐，覆盖重算窗口内的全部新增与更正。
* **不可用而非失败**：缺映射/单位不符只影响标准行的 `available` 与
  `reason`，原始消息的 AA、原始结果与回执完全不受影响。
