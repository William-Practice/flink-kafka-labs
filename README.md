# Flink + Kafka 实时计算实验（Flink 1.17 / Kafka 3.2）

[![CI](https://github.com/William-Practice/flink-kafka-labs/actions/workflows/ci.yml/badge.svg)](https://github.com/William-Practice/flink-kafka-labs/actions/workflows/ci.yml)
![Flink](https://img.shields.io/badge/Flink-1.17.2-E6526F?logo=apacheflink&logoColor=white)
![Kafka](https://img.shields.io/badge/Kafka-3.2.0-231F20?logo=apachekafka&logoColor=white)
![Java](https://img.shields.io/badge/Java-8%20%2F%2011-orange?logo=openjdk&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green)

> 一套**在真实集群上跑通、并留有实测输出**的 Flink 学习实验，不是照抄教程的示例代码。
> 覆盖：并行度与算子链 · Kafka 集成 + KeyedState + Checkpoint · Watermark + Trigger · Flink SQL（Kafka 源汇 / 窗口聚合 / JDBC sink）。
> 每个实验都附**实测结论和踩坑记录**（见 [`docs/flink-lab-notes.md`](docs/flink-lab-notes.md)）。

---

## 一、实验清单

| 文件 | 主题 | 演示什么 | 关键实测结论 |
|------|------|---------|-------------|
| [`L36ParallelismDemo.java`](src/main/java/com/example/L36ParallelismDemo.java) | 并行度 | 算子级 / 提交级(`-p`) / 环境级 / 配置文件 四级优先级 + 算子链 | `-p` **只影响没显式设并行度的算子**；`fromElements` 是非并行 Source，`-p` 改不动它 |
| [`L37KafkaStateCheckpoint.java`](src/main/java/com/example/L37KafkaStateCheckpoint.java) | State + Checkpoint | Kafka 入 → 切词 → `keyBy` → `ValueState` 累加 → Kafka 出；5s checkpoint | 计数递增到 `apple,9`；`flink-kafka-out` 24 条；checkpoint 实落到 `chk-12` |
| [`L39WatermarkTrigger.java`](src/main/java/com/example/L39WatermarkTrigger.java) | Watermark + Trigger | 有界 `fromElements` + 乱序时间戳 + 5s 事件时间窗口 + 自定义 Trigger | 踩到 3 个坑：非键控流不能注册定时器、水位线周期发射、有界流末尾补发 MAX |
| [`L39bWatermarkKafka.java`](src/main/java/com/example/L39bWatermarkKafka.java) | Watermark（真实节奏） | 同上但数据源换成 Kafka，按真实节奏灌数据 | 水位线**一个台阶一个台阶推进** `-1 → 1499 → 2999 → 4499 → 5999`，窗口在流中途触发 |
| [`sql/l41-l46-flink-sql.sql`](sql/l41-l46-flink-sql.sql) | Flink SQL | Kafka 源表（带 WATERMARK）/ TUMBLE 窗口聚合 / Kafka 汇表 / MySQL 汇表 | Kafka 侧与 MySQL 侧 **9 行结果逐行一致** |
| [`sql/l45-mysql-sink.sql`](sql/l45-mysql-sink.sql) | JDBC sink | Flink SQL 写 MySQL（单独一条，便于排查） | MySQL 方言不支持 `TIMESTAMP_LTZ`，需 `TIMESTAMP(3)` + CAST |

---

## 二、环境要求

| 组件 | 版本 | 说明 |
|------|------|------|
| Flink | 1.17.2（Scala 2.12） | standalone 模式，Web UI 端口本仓库实验里是 `8083` |
| Kafka + ZooKeeper | 3.2.0 / 3.8.4 | 单节点即可 |
| MySQL | 8.0 | 仅 Flink SQL 的 JDBC sink 需要 |
| JDK | 8 或 11 | Flink 1.17 支持 Java 8 / 11 |

**必须手动补的依赖**（Flink 发行包**不自带** connector）：

```bash
# Kafka connector —— 本仓库 L36 之后所有实验的前提，缺了会报 ClassNotFoundException
curl -fL -o $FLINK_HOME/lib/flink-sql-connector-kafka-1.17.2.jar \
  https://maven.aliyun.com/repository/public/org/apache/flink/flink-sql-connector-kafka/1.17.2/flink-sql-connector-kafka-1.17.2.jar

# Flink SQL 写 MySQL 需要（L45）
#   flink-connector-jdbc-3.1.2-1.17.jar  +  mysql-connector-j-8.0.33.jar
```

> ⚠️ 装完 connector 必须**重启 Flink 集群**（改 `lib/` 不重启不生效）。

---

## 三、怎么跑

### 1) 编译（本仓库的 CI 也只做这一步）

```bash
mvn -B compile        # 依赖全部 provided，只校验业务代码能编译
```

### 2) 打包 & 提交作业

```bash
# 打一个只含业务类的小 jar
jar cf flink-demos.jar -C target/classes com

# 提交（-p 指定并行度）
$FLINK_HOME/bin/flink run -c com.example.L36ParallelismDemo -p 4 flink-demos.jar

# 有界作业会自己结束；无界作业（Kafka 源）用 REST 取消
curl -s -X PATCH "localhost:8083/jobs/<jobid>?mode=cancel"
```

### 3) 实验用到的 Kafka topic

```bash
K=/opt/kafka/bin
$K/kafka-topics.sh --create --topic flink-kafka-in  --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
$K/kafka-topics.sh --create --topic flink-kafka-out --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
$K/kafka-topics.sh --create --topic flink-l39-in    --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092  # L39b 用单分区，便于观察水位线
$K/kafka-topics.sh --create --topic flink-sql-in    --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
$K/kafka-topics.sh --create --topic flink-sql-out   --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
```

### 4) Flink SQL

```bash
# 跑之前先把占位密码换成你自己的 MySQL 密码（Flink SQL 不支持读环境变量）
sed -i "s/CDC_PASSWORD/你的密码/" sql/*.sql

$FLINK_HOME/bin/sql-client.sh embedded -f sql/l41-l46-flink-sql.sql
```

---

## 四、实测输出摘录

### 4.1 并行度：`-p` 只影响"没设过的算子"

```
# flink run -p 1
Source: Collection Source -> stage0-env-default  (1/1)
stage1-p2  (1/2) (2/2)      stage2-p3  (1/3) (2/3) (3/3)

# flink run -p 4
Source: Collection Source  (1/1)
stage0-env-default  (1/4) (2/4) (3/4) (4/4)     ← 只有它跟着 -p 变
stage1-p2  (1/2) (2/2)      stage2-p3  (1/3) (2/3) (3/3)
```
（`(1/2)` 的读法是"2 个并行子任务里的第 1 个"。）

### 4.2 Watermark 逐步推进（L39b，每 1.5s 灌一条，乱序容忍 3s）

```
元素 a  eventTs=0     → watermark=-9223372036854775808   (Long.MIN_VALUE，还没发过水位线)
元素 a  eventTs=3000  → watermark=-1501
元素 c  eventTs=4500  → watermark=-1
定时器 @0    触发 ⇒ watermark=1499
定时器 @1500 触发 ⇒ watermark=2999
定时器 @4500 触发 ⇒ watermark=5999
[TRIGGER] onEventTime(watermark=4999) 窗口[0,5000) → FIRE_AND_PURGE
窗口[0,5000) key=a count=2 / key=b count=1 / key=c count=1
```

**窗口是在流还没结束时触发的** → 证明事件时间窗口由水位线驱动，而不是"输入结束才结算"。
水位线公式实测为 `maxEventTs - 乱序容忍 - 1ms`。

### 4.3 Flink SQL：窗口聚合双写 Kafka + MySQL

```
"1970-01-01 00:00:00Z","1970-01-01 00:00:05Z",apple,2      ← flink-sql-out
1970-01-01 00:00:00.000  1970-01-01 00:00:05.000  apple 2  ← word_count_result (MySQL)
...（共 9 行，两侧逐行一致）
```

---

## 五、关于密码

`sql/*.sql` 里的 MySQL 密码用占位符 `CDC_PASSWORD`，**不入库**。
跑之前用上面的 `sed` 换成你自己的密码即可。

---

## 六、相关

- 详细踩坑与原理笔记：[`docs/flink-lab-notes.md`](docs/flink-lab-notes.md)
- 姊妹项目（实时数仓）：[taxi-realtime-warehouse](https://github.com/William-Practice/taxi-realtime-warehouse) —— Flink CDC + Paimon 湖仓 + MySQL serving + WebSocket 大屏
