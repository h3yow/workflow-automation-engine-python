# 本地工作流自动化执行引擎（Python / FastAPI）

一个嵌入在 Web 服务进程内的 DAG 工作流引擎：步骤按依赖关系推进，在**并发触发、
步骤失败、定义变更**三类情况下都给出可预期的结论。

* 依赖推进：步骤的**全部**前置步骤成功后才允许执行；
* 触发幂等：同一运行的并发/重复触发只有一次真正执行，其余得到可区分的拒绝；
* 失败与重试：可恢复失败按指数退避自动重试且有上限；不可恢复失败立即可终止，
  失败步骤及其下游进入可区分终态，任何分支都不会被误标成功；
* 定义版本隔离：运行在触发时对定义做不可变快照，运行中的定义变更不影响在途运行。

## 目录结构

```
workflow_engine/
  models.py       # 状态枚举、定义/运行数据模型、可区分错误类型
  definitions.py  # 定义校验（缺失信息 / 自相矛盾 -> 可区分原因）
  store.py        # 内存存储 + 每运行一把 RLock（并发安全的根基）
  executor.py     # 处理器注册表；有上限的指数退避重试
  engine.py       # 编排核心：触发仲裁、DAG 调度、失败级联、快照绑定
main.py           # FastAPI 接口 + 开箱即用的 demo 处理器
tests/
  test_concurrent_trigger.py   # 场景 1：并发重复触发
  test_retry_failure.py        # 场景 2：可恢复重试 / 不可恢复失败级联
  test_definition_versioning.py# 场景 3：运行中定义变更 + 坏定义拒绝
  test_api.py                  # HTTP 端到端
```

## 运行状态流转

**运行（Run）状态**

```
                create_run                trigger（首个被接受的触发）
   (不存在) ─────────────────▶ PENDING ───────────────────────────────▶ RUNNING
                                  ▲                                      │
                     触发时工作流不存在则保持 PENDING                     │ 全部步骤 SUCCEEDED
                                  │                                      ▼
                                  │                                  SUCCEEDED（终态）
                                  │ 任一步骤 FAILED
                                  ▼                                      ▼
                              （可再次触发）                           FAILED（终态）
```

* 只有 `PENDING` 的运行可以接受触发；`RUNNING / SUCCEEDED / FAILED` 上的触发一律
  拒绝，原因 `already_running`，HTTP 409。
* 触发时若工作流尚未注册，返回 `workflow_not_found`，运行保持 `PENDING`（可稍后重试）。

**步骤（Step）状态**

```
PENDING ──全部前置 SUCCEEDED──▶ RUNNING ──成功──▶ SUCCEEDED（终态）
   │                              │
   │                              ├──可恢复错误且未超上限──▶ 退避后重试（仍为 RUNNING）
   │                              └──不可恢复错误 / 重试耗尽──▶ FAILED（终态）
   │
   └──失败级联：
        ├─ 失败步骤的（传递）下游  ──▶ SKIPPED（error 标注 basis=upstream_failure）
        └─ 其余尚未启动的独立步骤  ──▶ SKIPPED（error 标注 basis=run_aborted）
```

* 失败时引擎**停止调度一切尚未完成的步骤**，因此不会留下 PENDING，也不会把任何
  未完成的分支标记为 SUCCEEDED。
* 失败原因可追溯：`RunRecord.steps[step_id].error` 记录原始异常或
  `skipped: upstream step '<失败步骤>' failed`，直接定位到具体步骤。
* `SKIPPED` 与 `FAILED` 是两个可区分的终态：失败步骤本身是 FAILED，被连坐的下游
  是 SKIPPED；运行只要存在 FAILED/SKIPPED 即为 FAILED。

## 重试与幂等规则

**重试（仅针对可恢复失败）**

* 处理器抛出 `workflow_engine.executor.RetryableStepError` 表示可恢复失败；
  其它任何异常都视为不可恢复，**立即终止且不消耗重试次数**。
* 退避公式：第 `n` 次尝试前（n 从 2 起）等待
  `min(base_delay * 2**(n-2), max_delay)`；`max_attempts` 是**包含首次在内的
  总尝试次数上限**，达到上限仍失败则步骤 FAILED。
* 处理器正常返回即视为该步成功，输出保存在 `StepRuntime.output`。

**触发幂等（副作用绝不重复）**

* 每个运行有一把独立的 `RLock`。触发时在锁内完成「判定状态 → 绑定定义快照 →
  置为 RUNNING → 执行全部步骤 → 置终态」的完整读改写过程。
* 并发触发中只有一个线程看到 PENDING 并执行；其余线程在锁上等待，拿到锁后看到
  RUNNING 或终态，统一收到拒绝 `already_running`。
* 已结束的运行（SUCCEEDED/FAILED）再次触发同样被拒绝，**不会重放任何步骤副作用**。
* 拒绝不是静默吞掉：异常 `TriggerRejected.reason` / HTTP 响应体 `reason`
  可区分（`already_running`、`run_not_found`、`run_already_exists`、
  `workflow_not_found`）。
* `RunRecord.trigger_count` 记录所有触发尝试次数（含被拒绝的），便于审计断言。

## 定义版本兼容策略

* `register_definition` 时立即校验（见下）；校验失败整体拒绝，不改变当前生效版本。
* **触发时绑定**：`trigger` 把当时最新版本**深拷贝**进 `RunRecord.definition`。
  之后注册新版本不会影响在途或已结束的运行；新触发的运行才使用新版本。
* 同一 `(工作流名, 版本号)` 用不同内容重复注册会被拒绝（`version_conflict`），
  避免“同版本不同语义”破坏快照一致性；注册更高版本生效，注册更旧版本只入库不生效。

**被拒绝的定义（`DefinitionError.reason`，HTTP 422，可区分原因）**

| reason | 含义 |
| --- | --- |
| `missing_name` | 工作流名为空 |
| `empty_workflow` | 没有任何步骤 |
| `missing_step_id` | 存在空 id 的步骤 |
| `duplicate_step_id` | 步骤 id 重复 |
| `missing_handler` | 步骤未指定处理器 |
| `unknown_dependency` | 依赖了不存在的步骤 |
| `cyclic_dependency` | 依赖图存在环（Kahn 算法检测） |
| `invalid_retry_policy` | `max_attempts < 1` 或退避时间为负 |
| `version_conflict` | 同版本号内容被改动后重复注册 |

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/workflows` | 注册/升级一个工作流版本 |
| POST | `/runs/{run_id}` | 创建运行（body: `{"workflow_name": "..."}`），此时不绑定版本 |
| POST | `/runs/{run_id}/trigger` | 触发执行；重复/并发触发返回 409 + `already_running` |
| GET  | `/runs/{run_id}` | 查询运行状态、每步状态/尝试次数/错误 |

触发是同步执行：响应返回时运行已处于终态。

### 快速体验（内置 demo 处理器：`demo_ok` / `demo_flaky` / `demo_fatal`）

```bash
uv run fastapi dev main.py
# 另一个终端：
curl -s -X POST localhost:8000/workflows -H 'content-type: application/json' -d '{
  "name": "demo", "version": 1,
  "steps": [
    {"id": "a", "handler": "demo_ok"},
    {"id": "b", "depends_on": ["a"], "handler": "demo_flaky",
     "retry": {"max_attempts": 3, "base_delay_seconds": 0.05}}
  ]}'
curl -s -X POST localhost:8000/runs/run-1 -H 'content-type: application/json' \
  -d '{"workflow_name": "demo"}'
curl -s -X POST localhost:8000/runs/run-1/trigger
curl -s -X POST localhost:8000/runs/run-1/trigger   # 409 already_running
curl -s localhost:8000/runs/run-1
```

## 本地验证方法

```bash
uv sync                       # 安装依赖（含 dev 组的 pytest）
uv run pytest                 # 全部测试，-s 已默认开启，日志直接打印
uv run pytest -k retry        # 只跑重试/失败场景
uv run pytest -k concurrent   # 只跑并发重复触发场景
uv run pytest -k versioning   # 只跑定义变更/校验场景
uv run fastapi dev main.py    # 手工 HTTP 验证
```

测试覆盖三类必测场景，并断言**最终状态**与**副作用执行次数**：

1. `test_concurrent_duplicate_triggers_execute_side_effect_once`
   —— 8 个线程竞争 + 1 个运行中阻塞触发 + 1 个完成后重复触发：恰有 1 次接受、
   9 次 `already_running` 拒绝，处理器副作用恰好执行 1 次。
2. `test_recoverable_failure_retries_with_backoff_and_succeeds`
   / `..._exhausting_attempts_fails_terminal`
   / `test_unrecoverable_failure_marks_failed_and_skips_downstream`
   —— 退避序列 `[0.1, 0.2]`、尝试次数上限、失败步骤 FAILED、直接/传递下游
   SKIPPED(upstream_failure)、独立未启动分支 SKIPPED(run_aborted)。
3. `test_inflight_run_keeps_startup_definition_new_run_uses_new_version`
   —— 在途运行卡在步骤中间时注册 v2：旧运行仍按 v1（1 次尝试）失败，新运行按
   v2（3 次尝试）恢复成功；另含 7 种坏定义 + `version_conflict` 的拒绝原因断言。

每条断言都通过 `tests/_helpers.py` 打印 `run_id`、`step_id` 与判定依据 `basis=`，
引擎自身的日志同样带运行标识、步骤标识与判定依据（`basis=...`）。

## 设计边界

* 存储为**单进程内存态**，重启即清空；多进程/持久化部署需要替换 `RunStore`
  （锁与快照语义需在存储层重新保证）。
* 每个运行在单个工作线程内同步调度（步骤按定义顺序推进就绪集）；跨步骤并行不在
  当前范围内，但触发级别的并发安全由每运行锁保证。
