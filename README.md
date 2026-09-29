# 本地工作流自动化执行引擎（Python / FastAPI）

在现有 FastAPI 服务上实现的一套**本地**工作流自动化执行引擎，用于编排按依赖关系
（DAG）组织的步骤，在**一次运行内部并发**调度，并在并发触发、步骤失败重试与运行期
定义变更等情况下提供可预期的运行结论。

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
# max_concurrency 设置“一次运行内部”最多同时在跑的步骤数
registry.put(build_definition("demo", 1, [
    {"id": "validate"},
    {"id": "charge"},
    {"id": "notify"},               # 与 charge 互不依赖，可与其同时跑
    {"id": "ship", "depends_on": ["validate"]},
], max_concurrency=2))

actions = ActionRegistry()
actions.register("validate", lambda ctx: ...)
actions.register("charge",   lambda ctx: ...)

engine = WorkflowEngine(registry, actions, sleep_protocol=InstantSleep())
state = engine.trigger("run-001", wait=True)   # 或异步触发后 wait_until_done
print(state.status.value, {s: st.status.value for s, st in state.steps.items()})
```

## 2. 运行状态流转

### 2.1 运行（Run）状态

```
触发成功
   │
   ▼
 running ──全部已开步骤到终态且无失败──▶ succeeded（终态）
   │
   └──────────任一步骤 FAILED──────────▶ failed（终态，收敛后无悬空步骤）
```

### 2.2 步骤（Step）状态

```
pending ──全部前置 SUCCEEDED 且有并发空位──▶ running ──成功──▶ succeeded（终态）
                                                  │
                                                  ├── 不可恢复失败（FatalStepError/未声明异常）──▶ failed（终态）
                                                  │
                                                  └── 可恢复失败（RecoverableStepError）
                                                         ├─ 未达次数上限 ─退避等待─▶ running（重试）
                                                         └─ 达到上限 ────────────▶ failed（终态）

前置失败/被跳过后不再启动，或失败屏障下未启动 ──▶ skipped（终态，failed_by_step 记录阻断来源）
```

规则要点：

- 步骤是否能开始，只取决于两件事：**全部前置步骤都已 SUCCEEDED**，以及
  **当前有空闲的并发名额**。两个条件同时满足才会被调度（见第 3 节）。
- 失败原因通过 `StepState.error` 追溯到**具体步骤**；被跳过的步骤通过
  `failed_by_step` 记录阻断链上的来源步骤，可沿链一路回溯到真正 FAILED 的根因。
  与失败链无关、纯粹因“失败后不再开新步骤”而未启动的独立步骤，其
  `failed_by_step` 直接指向本次运行的根因失败步骤（拓扑序上最早的 FAILED）。
- 步骤 `attempts` 记录真实尝试次数（首次执行 + 重试次数）。
- `failed` 与 `skipped` 是两个**可区分的终止状态**，与 `succeeded` 三者互斥；
  运行收敛时**不存在**停留在 `pending/ready/running` 的悬空步骤。

## 3. 运行内部并发调度

### 3.1 调度规则

一次运行在一个调度线程内运行，具体步骤在该运行私有的线程池中执行：

- **无依赖的步骤并行**：彼此没有依赖关系的步骤可同时推进，独立分支不再互相干等。
- **有依赖严格等待**：一个步骤必须等它依赖的**全部**前置都 SUCCEEDED 才开始；
  上游产物以 `inputs` 透传给下游。依赖图按 Kahn 拓扑序计算，候选步骤按稳定的
  拓扑顺序填充并发空位，因此相同定义下调度次序确定、可复现。
- **并发度有上限、可配置、有默认值**：同时在跑的步骤数绝不超过
  `max_concurrency`；名额占满后，其余步骤排队等待空位，空出一个补一个，
  不会无限制地开线程。
- 不同运行各自独立线程池，互不占用对方名额。

### 3.2 并发上限的配置与默认值

- 在定义版本上配置 `max_concurrency`（正整数，`>= 1`）：
  `build_definition(..., max_concurrency=8)`，或
  `POST /definitions` 请求体的 `"max_concurrency": 8`。
- **不配置时使用向后兼容默认值 `DEFAULT_MAX_CONCURRENCY = 4`**。
  已经发布过的、没有该字段的旧版本定义仍然可以正常触发，并按默认并发度运行。
- 触发运行的瞬间，引擎把当时定义版本的生效并发度**解析并锁定**到
  `RunState.max_concurrency`（运行查询接口也会返回该值）。运行一旦开始，
  之后再发布任何新版本都**不影响这次在途运行**；新并发度只对之后触发的运行生效。
- 非法值（`0`、负数、非整数、布尔值）在**发布定义时**即被明确拒绝，错误原因码为
  `invalid_concurrency`，可与其它定义校验失败（`invalid_definition`）区分；
  HTTP 层两者都返回 `422`，仅 `error.reason` 不同。

### 3.3 失败收敛与状态判定

- 任一步骤 FAILED（不可恢复失败，或重试耗尽）后，运行最终以 **FAILED** 收尾，
  并拉起**失败屏障：不再启动任何新步骤**（包括排队中的独立步骤）。
- **失败前已经 SUCCEEDED 的步骤保持成功，不会被回退或重标。**
- 失败那一刻**已经在跑**的、与失败无依赖关系的其它分支，不强行中断，而是让它们
  跑到终态，规则一致：成功即 SUCCEEDED，自身失败即 FAILED（各自保留独立的 error）。
- 等所有在跑步骤都到终态后统一收敛：仍停留在 `pending`（从未启动）的步骤一律
  标记为 **SKIPPED**，并写好可追溯的 `failed_by_step`；不会有“跑到一半没结果”或
  “一直没开始”的悬空步骤。
- 根因归属是确定的：多个分支同时失败时，`failed_by_step` 链最终回溯到拓扑序上
  最早的 FAILED 步骤；相同定义 + 相同动作重复触发，运行成败与每步终态结论一致。

## 4. 重试与幂等规则

### 4.1 重试 / 退避

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

### 4.2 触发幂等（并发 / 重复触发）

- “检查 run_id 是否已存在 + 登记运行”在引擎全局锁内**原子**完成。
- N 个线程用同一个 `run_id` 并发触发：**恰好 1 个被接受**，其余全部抛出
  `DuplicateTriggerError`（HTTP 层映射为 `409`，响应体
  `error.reason="duplicate_trigger"`）——是**可区分的拒绝结果**，不会被静默吞掉。
- 即使运行已经结束，再次用相同 `run_id` 触发仍会被拒绝。
- 被拒绝的触发不会启动任何执行线程，因此**任何步骤的副作用都不会因重复触发而执行两次**。
- 不同 `run_id` 的运行互不影响，各自在独立后台线程并发执行。

## 5. 定义版本兼容策略

- `WorkflowDef` 是**不可变快照**（frozen dataclass）；运行在触发瞬间绑定其定义对象
  （`RunState.definition`）与当时解析出的并发上限（`RunState.max_concurrency`），
  之后注册表发布任何新版本都**不影响在途运行**——在途运行始终按启动时的步骤集合、
  依赖关系、重试策略与并发度跑完。
- **并发上限随定义版本走**：不同版本可以配置不同的 `max_concurrency`；
  已发布过的旧版本（无该字段）仍可正常触发，按向后兼容默认值
  `DEFAULT_MAX_CONCURRENCY = 4` 执行。
- `DefinitionRegistry.put()` 只发布**新版本号**（版本号单调，不可覆盖旧版本）。
  新定义（含新并发度）**只对发布之后触发的运行**生效；历史快照通过
  `get_version(n)` 永久可取，从而保证运行结论可复盘。
- 发布前会进行定义校验，下列“缺失必要信息 / 自相矛盾”的定义会被拒绝
  （HTTP 统一 `422`，用不同的 `error.reason` 区分）：
  - 名称为空、版本号 `< 1`、步骤列表为空（`reason="invalid_definition"`）；
  - 步骤 id 为空或重复；
  - `depends_on` 引用不存在的步骤（悬空依赖）；
  - 依赖图存在环（含自依赖）；
  - 步骤级 `max_attempts < 1`，或退避参数非法（负时长、乘数 `< 1`）；
  - 重复发布已经存在的版本号（变更必须以新版本发布）；
  - **`max_concurrency` 非正整数 / 非整数 / 布尔值
    （`reason="invalid_concurrency"`）**，与其它校验失败明确区分。

## 6. HTTP 接口

| 方法 & 路径 | 说明 | 关键响应 |
|---|---|---|
| `POST /definitions` | 发布新定义版本（可带 `max_concurrency`） | `200`；非法定义 `422 invalid_definition` / `422 invalid_concurrency` |
| `POST /runs` | 触发运行（可带 `run_id`） | `202`（响应含锁定的 `max_concurrency`）；重复触发 `409 duplicate_trigger` |
| `GET /runs/{run_id}` | 查询运行与每步状态（含 `max_concurrency`） | `200`；不存在 `404 run_not_found` |
| `GET /health` | 健康检查 | `200` |

错误响应结构：`{"error": {"reason": "<可区分原因码>", "message": "<细节>"}}`。

## 7. 本地验证方法

```bash
uv sync
uv run pytest                                  # 全部用例
uv run pytest --log-cli-level=INFO             # 同时查看引擎判定日志
uv run pytest tests/test_concurrent_trigger.py # 场景一：并发重复触发
uv run pytest tests/test_retry_failure.py      # 场景二：可恢复失败重试 / 致命失败
uv run pytest tests/test_definition_versioning.py  # 场景三：运行中定义变更
uv run pytest tests/test_intra_run_concurrency.py  # 场景四：运行内部并发
uv run pytest tests/test_api.py                # HTTP 可区分错误码
```

测试覆盖与断言：

1. **并发重复触发**：16 线程同 `run_id` 竞争，断言恰有 1 个被接受、15 个收到
   `duplicate_trigger` 拒绝，且三个步骤副作用各执行恰好 1 次。
2. **可恢复失败重试**：断言前两次可恢复失败后按 `[0.1s, 0.2s]` 退避、第三次成功；
   持续失败时尝试 3 次封顶后置 `failed`、下游 `skipped` 且原因可追溯；
   致命失败只尝试 1 次，下游不执行。
3. **运行中定义变更**：在 v1 运行执行到一半时发布 v2（步骤集合变化），断言旧运行
   仍按 v1 完成、新运行按 v2 完成；并对 5 类非法定义与版本重复发布断言拒绝原因。
4. **运行内部并发**：
   - 互不依赖的步骤被观测到同时 `running`（确证并行而非串行）；
   - 5 个无依赖步骤在 `max_concurrency=2` 下，整个生命周期在跑峰值恰为 2、
     其余排队、空出后补位且全部成功；
   - 菱形 DAG 的汇聚步骤严格等待两个前置都成功后才开始，且 inputs 完整；
   - 失败时：根因 FAILED（原因可追溯）、失败前/在跑的无关分支保留 SUCCEEDED、
     排队与下游步骤全部 SKIPPED 且 `failed_by_step` 可沿链回溯到根因、
     不存在任何中间悬空态、三类终态可区分；在跑的另一分支自身也失败时
     根因归属仍确定；
   - 在途运行锁定启动版本的并发度（2），期间发布并发度 8 的 v2 不影响旧运行，
     新运行按 8 跑；未配置版本按默认值 4 跑，不同版本可不同；
   - `max_concurrency` 为 0/负数/布尔值在发布期被拒，`reason=invalid_concurrency`；
   - 相同定义 + 相同动作重复触发 5 次，运行成败与各步终态签名完全一致（确定性）。

单测日志（logger 名 `workflow_engine` / `workflow_engine.test`）中每条判定都打印
**运行标识 `run_id`、步骤标识 `step_id` 与判定依据 `reason=...`**，例如：

```
INFO run_id=run-retry-exhausted step_id=call_gateway attempt=3/3 reason=execute_action
ERROR run_id=run-retry-exhausted step_id=call_gateway attempts=3 reason=step_failed kind=retries_exhausted ...
INFO run_id=run-retry-exhausted step_id=send_receipt reason=step_skipped failed_by_step=call_gateway
WARNING run_id=run-concurrent-1 reason=duplicate_trigger_rejected existing_status=running ...
```

## 8. 代码结构

```
main.py                       # FastAPI 入口（uvicorn main:app）
workflow_engine/
  models.py                   # 状态枚举、数据结构、异常类型（含并发度默认值/校验异常）
  definitions.py              # 定义构建、DAG 与并发度校验、版本注册表
  executor.py                 # 动作注册表、单步骤执行、重试退避
  engine.py                   # 编排引擎：幂等触发、有上限的并发 DAG 调度、失败收敛
  api.py                      # HTTP 路由与可区分错误响应
tests/                        # 四类必备场景 + API + 冒烟测试
  test_intra_run_concurrency.py  # 运行内部并发：并行/上限/依赖等待/失败收敛/版本锁定
```
