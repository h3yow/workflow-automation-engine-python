# 本地工作流自动化执行引擎（Python / FastAPI）

在现有 FastAPI 服务上实现的一套**本地**工作流自动化执行引擎，用于编排按依赖关系
（DAG）组织的步骤，并在并发触发、步骤失败重试与运行期定义变更三种情况下提供
可预期的运行结论。

## 1. 快速开始

```bash
uv sync                       # 安装依赖（含 pytest 开发依赖）
uv run pytest                 # 本地验证：运行全部单测
uv run uvicorn main:app --reload   # 启动 HTTP 服务
```

编程式用法：

```python
from workflow_engine import (
    ActionRegistry, DefinitionRegistry, WorkflowEngine,
    build_definition, RecoverableStepError,
)
from workflow_engine.executor import SleepProtocol

class InstantSleep(SleepProtocol):  # 测试/本地：退避不真实睡眠
    def sleep(self, s): ...

registry = DefinitionRegistry()
# max_concurrency 随版本走；省略时该版本按引擎默认并发度（DEFAULT_MAX_CONCURRENCY=4）跑
registry.put(build_definition("demo", 1, [
    {"id": "validate"},
    {"id": "charge", "depends_on": ["validate"]},
], max_concurrency=4))

actions = ActionRegistry()
actions.register("validate", lambda ctx: ...)
actions.register("charge",   lambda ctx: ...)

# 也可用 default_max_concurrency=N 覆盖引擎默认值（仅对未显式配置的版本生效）
engine = WorkflowEngine(registry, actions, sleep_protocol=InstantSleep())
state = engine.trigger("run-001", wait=True)   # 或异步触发后 wait_until_done
print(state.status.value, state.max_concurrency,
      {s: st.status.value for s, st in state.steps.items()})
```

## 2. 运行状态流转

### 2.1 运行（Run）状态

```
触发成功
   │
   ▼
 running ──全部步骤 SUCCEEDED──▶ succeeded（终态）
   │
   └──────任一步骤 FAILED──────▶ failed（终态，其余未完成步骤 SKIPPED）
```

### 2.2 步骤（Step）状态

```
pending ──全部前置 SUCCEEDED 且有空闲名额──▶ ready ──▶ running ──成功──▶ succeeded（终态）
                                                          │
                                                          ├── 不可恢复失败（FatalStepError/未声明异常）──▶ failed（终态）
                                                          │
                                                          └── 可恢复失败（RecoverableStepError）
                                                                 ├─ 未达次数上限 ─退避等待─▶ running（重试）
                                                                 └─ 达到上限 ────────────▶ failed（终态）

任一前置 FAILED/SKIPPED，或运行已因别处失败收敛 ──▶ skipped（终态，failed_by_step 记录断裂来源）
```

规则要点：

- **只有全部前置步骤成功**，一个步骤才会变为 `ready`（依赖感知的并发调度）。
  `ready` 后还要拿到并发名额才真正进入 `running`；名额满时在就绪队列中排队。
- 失败原因通过 `StepState.error` 追溯到**具体步骤**；被跳过的步骤通过
  `failed_by_step` 记录其失败来源步骤，可一路回溯到根因步骤。
- 步骤 `attempts` 记录真实尝试次数（首次执行 + 重试次数）。
- 运行只要存在失败步骤即为 `failed`；在本引擎的“失败即收敛”语义下，
  一旦任一步骤失败就**不再派发任何新步骤**，最终所有未决步骤一律 `skipped`，
  **不会有任何步骤被误标为成功，也不会残留在 pending/ready/running 中间态**；
  已经在失败前完成、以及失败时已经在跑且自然跑完的成功步骤保留其真实
  `succeeded` 状态，不回退、不重标。
- `failed` 与 `skipped` 是两个**可区分的终止状态**，与 `succeeded` 三者互斥。

## 2.3 运行内部并发调度

一次运行内部不再是“一条线串到底”，而是由 `ConcurrentScheduler`（`scheduler.py`）
做**依赖感知 + 有界并发**调度：

- **可并行**：彼此没有依赖关系的步骤同时推进（例如
  `root → left/right → join` 中的 `left`、`right` 并行，`join` 仍等两者都成功）。
- **严格依赖**：一个步骤只有在**全部直接前置都 `succeeded`** 后才会被派发，
  任何一个前置 `failed/skipped` 它都不会执行。
- **有上限**：同时处于 `running` 的步骤数绝不超过本次运行锁定的
  `max_concurrency`；名额满时就绪步骤在 FIFO 就绪队列（按稳定拓扑序）中等待空位，
  不会无限制地开线程。
- 不同运行之间本来就在各自线程执行，天然并发；该上限是**单次运行内部**的上限。

### 2.3.1 失败收敛与状态判定（并发下口径一致）

1. 任一步骤最终失败：该步骤 `failed`（`error` 即根因），运行整体 `failed`，
   失败派发闸门立即关闭，**不再启动任何新步骤**。
2. 失败时**已经在跑**的其它分支：让其自然跑完，成功仍记 `succeeded`（成功不
   回退），自身也失败则记 `failed`；不会被强行打断而留下悬空状态。
3. 依赖链上尚未开始的下游：`skipped`，`failed_by_step` 取其**第一个
   failed/skipped 的直接前置**，可逐级回溯到根因。
4. 已排队/仍 `pending`、与根因无依赖关系的步骤：不再派发，统一 `skipped`，
   `failed_by_step` 直接记根因步骤（断裂点明确）。
5. 多个步骤并发失败时，**根因按稳定拓扑序中最靠前的失败步骤确定**，
   保证同样定义/动作下结论可重复；`skipped` 链的回溯同样确定。
   （独立步骤在失败瞬间是“恰好已在跑跑完”还是“尚未派发被跳过”，属于真实
   调度时序；二者都是明确终态，且回溯口径一致——运行的最终成败结论始终确定。）
6. 收敛阶段按拓扑序扫尾，保证**没有任何步骤停在中间态**。

### 2.3.2 并发上限的配置、默认值与版本兼容

- 配置随**定义版本**走：`WorkflowDef.max_concurrency: int | None`，
  `build_definition(..., max_concurrency=N)`；每个版本可以不同。
- 默认值 `DEFAULT_MAX_CONCURRENCY = 4`（定义/引擎均可用默认）。
  也可在构造引擎时覆盖：`WorkflowEngine(..., default_max_concurrency=N)`。
- **向后兼容**：已发布的旧版本没有该字段（`None`）时，触发运行按
  `DEFAULT_MAX_CONCURRENCY` 锁定执行，历史版本仍可正常触发。
- **在途锁定**：每次运行在**触发瞬间**把并发度固化到
  `RunState.max_concurrency`（定义值优先，否则引擎默认值）；运行期间再发布
  新版本（哪怕并发度不同）不影响该次运行，新值只对之后触发的运行生效。
- **发布期校验**：`max_concurrency` 若不是整数或 `< 1`，在发布定义时即被拒绝，
  抛 `InvalidConcurrencyError`（`reason="invalid_concurrency"`，HTTP `422`），
  可与其它 `invalid_definition` 校验失败区分（它是 `InvalidDefinitionError`
  的子类，旧的捕获逻辑仍然兼容）。

## 3. 重试与幂等规则

### 3.1 重试 / 退避

- 动作抛 `RecoverableStepError` 表示**可恢复失败**：引擎按指数退避自动重试。
  第 n 次尝试失败后的等待为
  `min(base_delay * backoff_multiplier ** (n-1), max_delay)`，
  默认 `base=0.1s`、倍率 `2.0`、上限 `30s`。
- 重试次数有**硬上限**：定义级 `RetryPolicy.max_attempts`（默认 3，含首次执行），
  可在步骤上用 `max_attempts` 单独覆盖。超过上限后步骤 `failed`，
  错误信息形如 `retries exhausted (3/3): recoverable_step_failure: <原因>`。
- 动作抛 `FatalStepError` 表示**不可恢复失败**：不重试，直接失败并传播下游。
- 动作抛出的任何**其它异常**被包装为不可恢复失败（`fatal_step_failure: ...`），
  避免未知异常被静默当成可重试。

注意：“幂等”针对的是**触发**，不是动作重放。重试意味着同一动作会真实执行多次，
动作自身需保证业务幂等；引擎保证的是每次触发只产生**一条**执行流水线，
重试次数受上限约束。

### 3.2 触发幂等（并发 / 重复触发）

- “检查 run_id 是否已存在 + 登记运行”在引擎全局锁内**原子**完成。
- N 个线程用同一个 `run_id` 并发触发：**恰好 1 个被接受**，其余全部抛出
  `DuplicateTriggerError`（HTTP 层映射为 `409`，响应体
  `error.reason="duplicate_trigger"`）——是**可区分的拒绝结果**，不会被静默吞掉。
- 即使运行已经结束，再次用相同 `run_id` 触发仍会被拒绝。
- 被拒绝的触发不会启动任何执行线程，因此**任何步骤的副作用都不会因重复触发而执行两次**。
- 不同 `run_id` 的运行互不影响，各自在独立后台线程并发执行。

## 4. 定义版本兼容策略

- `WorkflowDef` 是**不可变快照**（frozen dataclass）；运行在触发瞬间绑定其定义对象
  （`RunState.definition`），之后注册表发布任何新版本都**不影响在途运行**——
  在途运行始终按启动时的步骤集合、依赖关系、重试策略与**并发上限**跑完。
  并发上限同样在触发时锁定到 `RunState.max_concurrency`。
- `DefinitionRegistry.put()` 只发布**新版本号**（版本号单调，不可覆盖旧版本）。
  新定义**只对发布之后触发的运行**生效；历史快照通过 `get_version(n)` 永久可取，
  从而保证运行结论可复盘。
- **兼容范围**：没有 `max_concurrency` 字段的旧版本（视为 `None`）可继续发布与触发，
  运行按 `DEFAULT_MAX_CONCURRENCY`（= 4）执行；新字段为可选，旧调用方无需改造。
- 发布前会进行定义校验，下列“缺失必要信息 / 自相矛盾”的定义会被拒绝，
  统一抛 `InvalidDefinitionError`（`reason="invalid_definition"`，HTTP `422`）：
  - 名称为空、版本号 `< 1`、步骤列表为空；
  - 步骤 id 为空或重复；
  - `depends_on` 引用不存在的步骤（悬空依赖）；
  - 依赖图存在环（含自依赖）；
  - 步骤级 `max_attempts < 1`，或退避参数非法（负时长、乘数 `< 1`）；
  - 重复发布已经存在的版本号（变更必须以新版本发布）。
- 其中并发上限非法（非整数或 `max_concurrency < 1`）使用**独立原因码**
  `InvalidConcurrencyError`（`reason="invalid_concurrency"`，HTTP `422`），
  便于调用方与其它定义校验失败分别处理。

## 5. HTTP 接口

| 方法 & 路径 | 说明 | 关键响应 |
|---|---|---|
| `POST /definitions` | 发布新定义版本（可带 `max_concurrency`） | `200`；非法定义 `422 invalid_definition`；并发上限非法 `422 invalid_concurrency` |
| `POST /runs` | 触发运行（可带 `run_id`） | `202`（响应含锁定的 `max_concurrency`）；重复触发 `409 duplicate_trigger` |
| `GET /runs/{run_id}` | 查询运行与每步状态 | `200`（含 `max_concurrency`）；不存在 `404 run_not_found` |
| `GET /health` | 健康检查 | `200` |

错误响应结构：`{"error": {"reason": "<可区分原因码>", "message": "<细节>"}}`。

发布带并发上限的定义示例：

```bash
curl -X POST localhost:8000/definitions -H 'content-type: application/json' -d '{
  "name": "etl", "version": 3,
  "max_concurrency": 8,
  "steps": [{"id": "a"}, {"id": "b", "depends_on": ["a"]}]
}'
# max_concurrency 传 0 / 负数 -> 422 {"error": {"reason": "invalid_concurrency", ...}}
```

## 6. 本地验证方法

```bash
uv sync
uv run pytest                                  # 全部 29 个用例
uv run pytest --log-cli-level=INFO             # 同时查看引擎判定日志
uv run pytest tests/test_concurrent_trigger.py # 场景一：并发重复触发（不回归）
uv run pytest tests/test_retry_failure.py      # 场景二：可恢复失败重试 / 致命失败（不回归）
uv run pytest tests/test_definition_versioning.py  # 场景三：运行中定义变更（不回归）
uv run pytest tests/test_intra_run_concurrency.py  # 新增：运行内部有界并发
uv run pytest tests/test_api.py                # HTTP 可区分错误码（含 invalid_concurrency）
```

测试覆盖与断言：

1. **并发重复触发**：16 线程同 `run_id` 竞争，断言恰有 1 个被接受、15 个收到
   `duplicate_trigger` 拒绝，且三个步骤副作用各执行恰好 1 次。
2. **可恢复失败重试**：断言前两次可恢复失败后按 `[0.1s, 0.2s]` 退避、第三次成功；
   持续失败时尝试 3 次封顶后置 `failed`、下游 `skipped` 且原因可追溯；
   致命失败只尝试 1 次，下游不执行。
3. **运行中定义变更**：在 v1 运行执行到一半时发布 v2（步骤集合变化），断言旧运行
   仍按 v1 完成、新运行按 v2 完成；并对 5 类非法定义与版本重复发布断言拒绝原因。
4. **运行内部并发**（`test_intra_run_concurrency.py`，9 例）：
   - 三个互不依赖步骤经 `Barrier` 验证**确实并行**（峰值并发 = 3）；
   - 6 个独立步骤在 `max_concurrency=2` 下峰值并发**恒 ≤ 2 且用满 2**；
   - 菱形 DAG 中 `join` 的开始时刻晚于两个前置完成时刻，前置严格等待、分支并行；
   - 失败收敛：根因 `failed`（含原始错误信息）、失败链下游与排队步骤 `skipped`
     且 `failed_by_step` 可回溯到根因、在跑的无关成功分支保持 `succeeded`，
     **所有步骤均落在终态、无中间态残留**；
   - 在途运行锁定 v1 的并发上限，期间发布 v2（上限不同）不影响它；未配置上限的
     旧版本按 `DEFAULT_MAX_CONCURRENCY` 跑；
   - `max_concurrency` 为 `0/-1/-99` 发布时抛 `invalid_concurrency`，可与
     `invalid_definition` 区分；引擎默认上限非法也被拒绝；
   - 相同定义与动作重复触发 5 次，最终运行结论与每步终态完全一致。

单测日志（logger 名 `workflow_engine` / `workflow_engine.test`）中每条判定都打印
**运行标识 `run_id`、步骤标识 `step_id` 与判定依据 `reason=...`**，例如：

```
INFO run_id=run-retry-exhausted step_id=call_gateway attempt=3/3 reason=execute_action
ERROR run_id=run-retry-exhausted step_id=call_gateway attempts=3 reason=step_failed kind=retries_exhausted ...
INFO run_id=run-retry-exhausted step_id=send_receipt reason=step_skipped failed_by_step=call_gateway
WARNING run_id=run-concurrent-1 reason=duplicate_trigger_rejected existing_status=running ...
```

## 7. 代码结构

```
main.py                       # FastAPI 入口（uvicorn main:app）
workflow_engine/
  models.py                   # 状态枚举、数据结构、异常类型（含 DEFAULT_MAX_CONCURRENCY）
  definitions.py              # 定义构建、DAG/并发上限校验、版本注册表
  executor.py                 # 动作注册表、单步骤执行、重试退避
  scheduler.py                # 运行内部有界并发调度：依赖感知、满额排队、失败收敛
  engine.py                   # 编排引擎：幂等触发、调度器装配、定义/并发度快照
  api.py                      # HTTP 路由与可区分错误响应
tests/                        # 并发重复触发/重试/版本/运行内并发/API/冒烟
```
