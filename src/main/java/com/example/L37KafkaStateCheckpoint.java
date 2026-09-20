package com.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * L37 实验：Flink + Kafka + KeyedState + Checkpoint
 *
 * 链路：Kafka(flink-kafka-in) → 切词 → keyBy(word) → ValueState 累加计数 → Kafka(flink-kafka-out)
 * 关键点：
 *   1. 用 KeyedProcessFunction 里的 ValueState<Long> 保存"每词累计次数"（Flink 托管的键控状态，不是普通变量）
 *   2. 打开 checkpoint（5s 一次，落到 file:///home/wry/flink-checkpoints）
 *   3. sink 用 AT_LEAST_ONCE（配合 checkpoint 做一致性）
 *
 * 跑法：flink run -d 提交（Kafka source 是无界的，不会自己结束），灌数据后 flink cancel。
 */
public class L37KafkaStateCheckpoint {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // ---------- ① Checkpoint 配置（本节重点之一）----------
        env.enableCheckpointing(5000);                                  // 每 5 秒一次
        env.getCheckpointConfig().setCheckpointStorage("file:///home/wry/flink-checkpoints");
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION); // 取消作业也保留，便于验证
        System.out.println("[conf] checkpoint interval = 5000ms, storage = file:///home/wry/flink-checkpoints");

        // ---------- ② Kafka Source ----------
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("flink-kafka-in")
                .setGroupId("flink-l37-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // ---------- ③ Kafka Sink ----------
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("flink-kafka-out")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        DataStream<String> words = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source: flink-kafka-in")
                .flatMap(new Tokenizer())
                .name("split-words");

        // ---------- ④ 键控状态累加 ----------
        DataStream<String> counted = words
                .keyBy(new KeySelector<String, String>() {
                    @Override
                    public String getKey(String word) {
                        return word;
                    }
                })
                .process(new KeyedProcessFunction<String, String, String>() {
                    private ValueState<Long> countState;

                    @Override
                    public void open(Configuration parameters) {
                        countState = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("word-count", Long.class));
                    }

                    @Override
                    public void processElement(String word, Context ctx, Collector<String> out) throws Exception {
                        Long old = countState.value();
                        long now = (old == null ? 1L : old + 1L);
                        countState.update(now);                       // ← 状态写入（受 checkpoint 保护）
                        out.collect(String.format("%s,%d,subtask#%d", word, now,
                                getRuntimeContext().getIndexOfThisSubtask()));
                    }
                })
                .name("keyed-count-with-state");

        counted.print("L37-STATE").name("print-console");
        counted.sinkTo(sink).name("Kafka Sink: flink-kafka-out");

        env.execute("L37 Kafka + State + Checkpoint");
    }

    /** 按逗号切词 */
    public static class Tokenizer extends RichFlatMapFunction<String, String> {
        @Override
        public void flatMap(String line, Collector<String> out) {
            for (String w : line.split(",")) {
                String t = w.trim();
                if (!t.isEmpty()) {
                    out.collect(t);
                }
            }
        }
    }
}
