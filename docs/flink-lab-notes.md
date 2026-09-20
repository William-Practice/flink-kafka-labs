# Flink 实验笔记：原理、实测数据与踩坑

> 全部内容来自在 Flink 1.17.2 standalone（单节点，4 slot）+ Kafka 3.2.0 上的实际运行，
> 输出的数字和报错都是真跑出来的，不是从教程抄的。

---

## 一、并行度（对应 `L36ParallelismDemo`）

### 1.1 四级优先级

```
算子级 setParallelism()  >  提交级 flink run -p N  >  环境级 env.setParallelism(N)  >  配置文件 parallelism.default
```

实测（同一份代码跑两次）：

| 算子 | `-p 1` | `-p 4` |
|------|--------|--------|
| `Source: Collection Source` | `(1/1)` | `(1/1)` ← 不受影响 |
| `stage0-env-default`（未显式设置） | `(1/1)` | `(1/4) (2/4) (3/4) (4/4)` ← 跟着 `-p` 变 |
| `stage1-p2`（`setParallelism(2)`） | `(1/2) (2/2)` | `(1/2) (2/2)` ← 不变 |
| `stage2-p3`（`setParallelism(3)`） | `(1/3)…(3/3)` | `(1/3)…(3/3)` ← 不变 |
| `Sink: Print to Std. Out` | `(1/1)` | `(1/1)` |

### 1.2 三个容易读错/踩到的点

1. **`fromElements` 是非并行 Source**，并行度恒为 1，`-p` 改不动它。
   想验证 `-p` 的效果，必须留一个**不显式设并行度**的算子做对照。
2. **`(1/2)` 是"2 个并行子任务里的第 1 个"**，不是"并行度 = 1"。JM 日志里这个记法很容易误读。
3. **并行度变化会打断算子链**：`-p 1` 时 source 与 stage0 都是 1，日志显示
   `Source: Collection Source -> stage0-env-default (1/1)`（链在一起）；
   `-p 4` 时两者并行度不同，链不上，变成两个独立算子。

### 1.3 Slot 与并行度

- Slot 是 TaskManager 的资源单位；并行度是算子的并发实例数。
- 同一 slot sharing group 内各算子可共享 slot，所以作业需要的 slot 数 =
  **各算子并行度的最大值**（不是求和）。本实验 `max(1,4,2,3,1) = 4`，正好用满单机 4 slot。
- `taskmanager.numberOfTaskSlots` 默认只有 1，多算子作业容易报
  `NoResourceAvailableException: Could not acquire the minimum required resources`。

---

## 二、Kafka 集成 + KeyedState + Checkpoint（对应 `L37KafkaStateCheckpoint`）

### 2.1 API 选型

| | 旧 API | 新 API |
|---|---|---|
| Source | `FlinkKafkaConsumer`（1.17 已废弃） | `KafkaSource.builder()...` |
| Sink | `FlinkKafkaProducer` | `KafkaSink.builder()...` |

新 API 需要 Flink 的 Kafka connector jar；**Flink 发行包不自带**，必须自己放进 `$FLINK_HOME/lib/`。

### 2.2 键控状态

```java
countState = getRuntimeContext().getState(new ValueStateDescriptor<>("word-count", Long.class));
```

- 只有 `keyBy` 之后的流才有键控状态；状态按 key 隔离，由 Flink 托管并参与 checkpoint。
- 用普通成员变量做计数在故障恢复后会丢，用 `ValueState` 才能从 checkpoint 恢复。

### 2.3 Checkpoint

```java
env.enableCheckpointing(5000);
env.getCheckpointConfig().setCheckpointStorage("file:///home/wry/flink-checkpoints");
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000);
env.getCheckpointConfig().setCheckpointTimeout(60000);
env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);
```

实测：作业跑 57.3s，checkpoint 目录编号到 `chk-12`（≈ 每 5 秒一次）：

```
/home/wry/flink-checkpoints/<jobid>/
├── chk-12/_metadata      (7604 B)
├── shared/
└── taskowned/
```

**坑**：
- 默认只保留最近 1 个 checkpoint → `_metadata` 只找得到 1 份，但编号已经涨到 `chk-12`。
  要看历史要配 `state.checkpoints.num-retained`。
- `_metadata` **不是纯 JSON**（Flink 自有格式 + 二进制头），`python json.load` 会
  `UnicodeDecodeError`。
- checkpoint ≠ 故障恢复验证：想证明"能恢复"还得 `flink run -s <savepoint>` 或杀 TM 观察重启。

### 2.4 一致性

`setDeliveryGuarantee(AT_LEAST_ONCE)` 配合 checkpoint 使用。
要 `EXACTLY_ONCE` 需要 Kafka 事务（`setTransactionalIdPrefix`）+ checkpoint 完成时提交事务，
代价是延迟；本实验只演示 AT_LEAST_ONCE。

### 2.5 取消作业

`flink cancel <jobid>` 这个 CLI 偶发直接抛 CliFrontend 堆栈（没取消成功），走 REST 更稳：

```bash
curl -s -X PATCH "localhost:8083/jobs/<jobid>?mode=cancel"   # 返回 {} 即成功
```

---

## 三、Watermark + Trigger（对应 `L39WatermarkTrigger` / `L39bWatermarkKafka`）

### 3.1 水位线公式与节奏（实测）

`forBoundedOutOfOrderness(3s)` 时：**watermark = 见过的最大 eventTs - 3000 - 1ms**

对着数据验（每 1.5s 灌一条）：

```
元素 a  eventTs=3000  → watermark=-1501     (1500-3000-1)
元素 c  eventTs=4500  → watermark=-1        (3000-3000-1)
定时器 @0    触发 ⇒ watermark=1499          (4500-3000-1)
定时器 @1500 触发 ⇒ watermark=2999          (6000-3000-1)
定时器 @4500 触发 ⇒ watermark=5999          (9000-3000-1)
```

即"乱序容忍 3 秒"= 水位线永远落后真实时间 3 秒多一点。

### 3.2 三个必须知道的坑

**坑1：非键控流不能注册定时器**

```
java.lang.UnsupportedOperationException: Setting timers is only supported on a keyed streams.
        at ...ProcessOperator$ContextImpl.registerEventTimeTimer(...)
```
→ 要注册事件时间定时器，先 `keyBy`，用 `KeyedProcessFunction`。

**坑2：水位线是周期性发射的，不是每个元素都发**

`autoWatermarkInterval` 默认 200ms。用有界 `fromElements`（几毫秒就灌完）时，
`processElement` 里读 `currentWatermark()` **永远是 `Long.MIN_VALUE`**。
→ 观察水位线的正确姿势：注册事件时间定时器，在 `onTimer` 里读；或者换慢速/无界数据源。

**坑3：有界流末尾会补发一个 MAX 水位线**

所以最后所有定时器都读到 `9223372036854775807`、所有窗口被一次性冲出来。
这会让人误判成"窗口是输入结束才结算"，其实那是**有界流的特例**。
真正的验证要用无界源（本仓库的 Kafka 版 `L39b`）：窗口在流中途就触发了。

### 3.3 Trigger 的 4 种返回值

| 返回值 | 含义 |
|--------|------|
| `CONTINUE` | 什么都不做 |
| `FIRE` | 触发窗口计算，**保留**窗口内容（后续还能再触发） |
| `PURGE` | **清空**窗口内容，不触发计算 |
| `FIRE_AND_PURGE` | 触发计算并清空（窗口关闭的常见做法） |

**容易误判的现象**：`onEventTime` 打印看起来重复触发，其实是 ——
**Trigger 回调签名里没有 key**（只有 window），而窗口按 key 分组，
同一个窗口下每个 key 都会各自触发一次（本例 key=a/b/c → 3 次）。

### 3.4 Watermark vs allowedLateness

- **Watermark** 决定**什么时候**触发窗口（可以理解为"我认为这个时间点之前的数据都到齐了"）。
- **allowedLateness** 决定**触发之后还等多久**的迟到数据（能让窗口多次触发）。
  用 `sideOutputLateData` 把超期数据导到侧输出，别丢。

---

## 四、Flink SQL（对应 `sql/l41-l46-flink-sql.sql`、`sql/l45-mysql-sink.sql`）

### 4.1 源表带水位线

```sql
CREATE TABLE kafka_source (
    word        STRING,
    ts          BIGINT,
    event_time  AS TO_TIMESTAMP_LTZ(ts, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '3' SECOND
) WITH ('connector'='kafka', ..., 'format'='csv');
```

### 4.2 滚动窗口聚合

```sql
SELECT window_start, window_end, word, COUNT(*) AS cnt
FROM TABLE(TUMBLE(TABLE kafka_source, DESCRIPTOR(event_time), INTERVAL '5' SECONDS))
GROUP BY window_start, window_end, word;
```

实测（灌 13 条 → 3 个窗口 × 3 个词，Kafka 与 MySQL 两侧逐行一致）：

```
"1970-01-01 00:00:00Z","1970-01-01 00:00:05Z",apple,2
"1970-01-01 00:00:05Z","1970-01-01 00:00:10Z",apple,2
"1970-01-01 00:00:10Z","1970-01-01 00:00:15Z",apple,1
```

### 4.3 四个坑

1. **MySQL 方言不支持 `TIMESTAMP_LTZ`**
   ```
   ValidationException: The MySQL dialect doesn't support type: TIMESTAMP_LTZ(3) NOT NULL.
   ```
   → JDBC 汇表列用 `TIMESTAMP(3)`（= MySQL `DATETIME(3)`），SELECT 里
   `CAST(window_start AS TIMESTAMP(3))`。Kafka 汇表用 `TIMESTAMP_LTZ(3)` 没问题。
2. **多作业读同一 topic 必须错开 `group.id`**：单分区 topic 会被同组成员瓜分，
   只有一个作业收得到数据。
3. **窗口何时出现取决于水位线**：灌到 `ts=16000`（watermark=13000）时 `[10,15)` 窗口不出现，
   补灌到 `ts=19000` 后才出现 —— SQL 层和 DataStream 层（L39）验证的是同一件事。
4. **非交互 `sql-client.sh -f` 执行完 `INSERT` 后，会话退出 ≠ 作业取消**，
   作业继续在集群跑；要停用 `flink cancel` 或 REST。

### 4.4 结果模式

`SET 'sql-client.execution.result-mode' = 'table' | 'changelog' | 'tableau'`。
`changelog` 能看到 `+I / -U / +U / -D` 的变更流，是做流式调试最有用的模式。

---

## 五、环境层面的三个必知

1. **Flink 发行包不带任何 connector**（Kafka / JDBC / Hive / CDC 都得自己放进 `lib/`）。
   这是本仓库所有实验的前提，也是最容易卡住的一步。
2. **改了 `$FLINK_HOME/lib/` 必须重启集群**才生效。
3. 单机 4 slot 的环境下，多个无界作业同时跑会抢 slot；
   实验时一个作业一个作业来，或用 `flink list` 确认没有残留作业。
