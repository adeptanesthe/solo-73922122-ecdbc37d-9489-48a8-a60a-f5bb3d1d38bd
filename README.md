# LIMS 生化仪接口接收服务（HL7 v2.5.1 ORU^R01 / MLLP）

单进程接收服务：FastAPI + SQLite，MLLP TCP 端口只接收单台生化仪的 ASCII
HL7 v2.5.1 `ORU^R01`。每条消息一个 OBR、1～20 个不重复的数值 OBX；
样本号取 OBR-3.1，项目取 OBX-3.1，值取 OBX-5，状态取 OBX-11。

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
| `GET /api/v1/samples/{sample}` | 当前结果（项目、值、单位、状态、来源 MSH-10、更新时间）及该样本已接纳消息列表 |
| `GET /api/v1/samples?limit=` | 最近更新的样本及项目数 |
| `GET /api/v1/messages/{msh10}` | 消息原文、原文摘要、ACK 代码、持久化的 ACK 原文 |
| `POST /api/v1/demo/messages` | 经真实 MLLP 发送（分片 / 丢回执重发） |
| `POST /api/v1/demo/concurrent` | N 连接并发发送同一条消息 |
| `GET /api/v1/demo/samples/{sample}` | 纯文本结果摘要 |

## 本地开发与测试

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
pytest                      # 55 项：解析、拒绝、F/C、并发、重启、分片/粘包、HTTP、demo
./run_local.sh              # http://localhost:8000/docs，MLLP 2575，库在 ./data/lis.db
python -m app.simulator --host 127.0.0.1 --port 2575
```

环境变量：`LIS_DB_PATH`（默认 `/data/lis.db`）、`LIS_MLLP_HOST`（默认
`0.0.0.0`）、`LIS_MLLP_PORT`（默认 `2575`）。

## 设计取舍

* **单进程 + 单写者**：接收进程只有一个 asyncio 循环，写操作经
  `asyncio.to_thread` 交给 SQLite 单写事务，配合进程内锁与
  `busy_timeout` 串行化；不引入外部队列，保证"提交后才回 AA"。
* **回执持久化**：把生成的 ACK 字节与结果一起存库，重发时原样返回，
  避免重新生成 ACK 导致时间戳/MSH-10 不同而破坏"同 ID 同回执"。
* **指纹用规范化原文**：CR/LF 差异不算"异文"，其余任何字节差异都算。
* **数值校验从严**：OBX-2 非 NM/SN、OBX-5 非数值都整条拒绝，避免 LIS
  里出现无法计算的结果。
