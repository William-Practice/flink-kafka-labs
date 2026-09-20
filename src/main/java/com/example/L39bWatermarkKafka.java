package com.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * L39-b：用 Kafka 真实节奏的数据观察水位线"逐步"推进
 *
 * 为什么需要这个版本：L39 主体用的是有界 fromElements，几十毫秒就灌完了，
 * 水位线（默认 200ms 周期发射）压根来不及发，只能看到 Long.MIN_VALUE → 末尾 Long.MAX_VALUE。
 * 换成 Kafka 源 + 真实间隔灌数据，就能看到 watermark 一个台阶一个台阶往上走。
 *
 * 消息格式：word,eventTsMillis
 * 乱序容忍：3 秒 → watermark = 见过的最大 eventTs - 3000
 * 窗口：5 秒滚动事件时间窗口，key = word
 *
 * 建议灌数据（每秒一条，时间戳每次 +1500）：
 *   a,0  b,1500  a,3000  c,4500  a,6000  b,7500  c,9000  b,10500  a,12000
 */
public class L39bWatermarkKafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("flink-l39-in")
                .setGroupId("flink-l39b-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> events = env
                .fromSource(source, WatermarkStrategy.<String>noWatermarks(), "Kafka Source")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((element, ts) -> Long.parseLong(element.split(",")[1])))
                .name("watermark-strategy");

        // 观察水位线推进（必须 keyBy 才能注册定时器）
        events.keyBy(new KeySelector<String, String>() {
                    @Override
                    public String getKey(String v) {
                        return v.split(",")[0];
                    }
                })
                .process(new KeyedProcessFunction<String, String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) {
                        long ts = ctx.timestamp();
                        ctx.timerService().registerEventTimeTimer(ts);
                        out.collect(String.format("元素 %-4s eventTs=%-6d → 注册定时器（此刻 watermark=%d）",
                                value.split(",")[0], ts, ctx.timerService().currentWatermark()));
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) {
                        out.collect(String.format("定时器 @%-6d 触发 ⇒ 水位线已越过，此刻 watermark=%d",
                                timestamp, ctx.timerService().currentWatermark()));
                    }
                })
                .name("A-watermark-observer")
                .print("L39B-WATERMARK");

        // 事件时间窗口 + 自定义 Trigger
        events.map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String v) {
                        return Tuple2.of(v.split(",")[0], 1);
                    }
                })
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .trigger(new L39WatermarkTrigger.LoggingTrigger())
                .process(new ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context ctx, Iterable<Tuple2<String, Integer>> elements,
                                        Collector<String> out) {
                        int sum = 0;
                        for (Tuple2<String, Integer> e : elements) {
                            sum += e.f1;
                        }
                        out.collect(String.format("窗口[%d, %d)  key=%-3s count=%d  （触发时 watermark=%d）",
                                ctx.window().getStart(), ctx.window().getEnd(), key, sum,
                                ctx.currentWatermark()));
                    }
                })
                .name("B-event-time-window")
                .print("L39B-WINDOW");

        env.execute("L39b Watermark via Kafka");
    }
}
