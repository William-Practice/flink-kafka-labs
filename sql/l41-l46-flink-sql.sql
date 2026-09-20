-- ============================================================
-- L41~L46：Flink SQL 系列实验
--   L41 SQL 基础 / L42 Kafka 源汇表 / L43 窗口聚合
--   L44 维表与结果表 / L45 JDBC sink 写 MySQL / L46 连续查询
-- 运行：/opt/flink-1.17.2/bin/sql-client.sh embedded -f /home/wry/l41-l46-flink-sql.sql
-- ============================================================

SET 'execution.runtime-mode' = 'streaming';
SET 'sql-client.execution.result-mode' = 'table';
SET 'pipeline.name' = 'L41-L46 Flink SQL Demo';

-- ---------- ① Kafka 源表（带 Watermark，事件时间）----------
CREATE TABLE kafka_source (
    word        STRING,
    ts          BIGINT,
    event_time  AS TO_TIMESTAMP_LTZ(ts, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '3' SECOND
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'flink-sql-in',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id'          = 'flink-sql-group',
    'scan.startup.mode'            = 'earliest-offset',
    'format'                       = 'csv'
);

-- ---------- ② Kafka 汇表（窗口结果写回 Kafka）----------
CREATE TABLE kafka_sink (
    window_start TIMESTAMP_LTZ(3),
    window_end   TIMESTAMP_LTZ(3),
    word         STRING,
    cnt          BIGINT
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'flink-sql-out',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format'                       = 'csv'
);

-- ---------- ③ MySQL 汇表（JDBC sink）----------
-- ⚠️ MySQL 方言不支持 TIMESTAMP_LTZ，必须用 TIMESTAMP(3)（= MySQL DATETIME(3)），
--    并在 INSERT 的 SELECT 里显式 CAST。
CREATE TABLE mysql_sink (
    window_start TIMESTAMP(3),
    window_end   TIMESTAMP(3),
    word         STRING,
    cnt          BIGINT,
    PRIMARY KEY (window_start, word) NOT ENFORCED
) WITH (
    'connector'  = 'jdbc',
    'url'        = 'jdbc:mysql://localhost:3306/spark_test',
    'table-name' = 'word_count_result',
    'username'   = 'cdc',
    'password'   = 'CDC_PASSWORD'
);

-- ---------- ④ 连续查询1：5 秒滚动窗口按词计数 → Kafka ----------
INSERT INTO kafka_sink
SELECT window_start, window_end, word, COUNT(*) AS cnt
FROM TABLE(
        TUMBLE(TABLE kafka_source, DESCRIPTOR(event_time), INTERVAL '5' SECONDS)
     )
GROUP BY window_start, window_end, word;

-- ---------- ⑤ 连续查询2：同样的窗口结果 → MySQL ----------
INSERT INTO mysql_sink
SELECT CAST(window_start AS TIMESTAMP(3)) AS window_start,
       CAST(window_end   AS TIMESTAMP(3)) AS window_end,
       word,
       cnt
FROM (
    SELECT window_start, window_end, word, COUNT(*) AS cnt
    FROM TABLE(
            TUMBLE(TABLE kafka_source, DESCRIPTOR(event_time), INTERVAL '5' SECONDS)
         )
    GROUP BY window_start, window_end, word
);
