-- ============================================================
-- L45：Flink SQL → MySQL（JDBC sink）
--
-- ⚠️ 坑：MySQL 方言**不支持 TIMESTAMP_LTZ**，直接建表会报
--    ValidationException: The MySQL dialect doesn't support type: TIMESTAMP_LTZ(3) NOT NULL.
--    解法：汇表用 TIMESTAMP(3)（对应 MySQL DATETIME(3)），并在 SELECT 里 CAST 一下。
--
-- ⚠️ 另注意：group.id 要跟其它读同一个 topic 的作业错开，
--    否则单分区 topic 会被组内成员瓜分（只有一个能拿到）。
-- ============================================================

SET 'execution.runtime-mode' = 'streaming';
SET 'pipeline.name' = 'L45 Flink SQL to MySQL';

CREATE TABLE kafka_source (
    word        STRING,
    ts          BIGINT,
    event_time  AS TO_TIMESTAMP_LTZ(ts, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '3' SECOND
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'flink-sql-in',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id'          = 'flink-sql-mysql-group',
    'scan.startup.mode'            = 'earliest-offset',
    'format'                       = 'csv'
);

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
