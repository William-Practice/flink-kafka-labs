package com.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
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
 * L39 实验：Watermark（水位线）+ Trigger（触发器）
 *
 * 数据（毫秒时间戳，故意乱序）：
 *   a@0  b@1000  a@3000  c@6000  a@2000  b@7000  c@8000  b@4000  a@9000  c@11000  b@12000
 *   END@60000   ← 哨兵事件：把水位线推过所有窗口，逼所有窗口收尾
 *
 * 支路A：观察水位线推进。**注意不能直接在 processElement 里读 currentWatermark**：
 *        水位线是**周期性发射**的（默认 200ms 一次），而这份有界数据几毫秒就灌完了，
 *        元素处理时一条水位线都还没发出，读到的永远是 Long.MIN_VALUE（-9223372036854775808）。
 *        正确姿势：注册**事件时间定时器**，等水位线越过该时间戳时回调里再读。
 *
 * 支路B：keyBy(word) → 5 秒滚动事件时间窗口 → 自定义 Trigger（打印回调并返回 FIRE_AND_PURGE）
 *        → ProcessWindowFunction 打印窗口起止时间与计数。
 *
 * Trigger 的 4 种返回值（笔记里的对照表）：
 *   CONTINUE        什么都不做
 *   FIRE            触发窗口计算，但保留窗口内容（后续还能再触发）
 *   PURGE           清空窗口内容，但不触发计算
 *   FIRE_AND_PURGE  先触发计算再清空（本实验采用）
 */
public class L39WatermarkTrigger {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);                                 // 便于观察顺序
        env.getConfig().setAutoWatermarkInterval(100);          // 水位线发射间隔：默认 200ms → 100ms

        DataStream<String> raw = env.fromElements(
                "a,0", "b,1000", "a,3000", "c,6000", "a,2000", "b,7000",
                "c,8000", "b,4000", "a,9000", "c,11000", "b,12000", "END,60000");

        // 乱序容忍 3 秒
        DataStream<String> events = raw.assignTimestampsAndWatermarks(
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((element, recordTs) -> {
                            String[] p = element.split(",");
                            return Long.parseLong(p[1]);
                        })
        ).name("watermark-strategy");

        // ---------- 支路A：用事件时间定时器观察水位线 ----------
        // ⚠️ 定时器只能在**键控流**上注册：直接对非键控流 process() 会抛
        //    java.lang.UnsupportedOperationException: Setting timers is only supported on a keyed streams.
        //    所以先 keyBy（这里按词分键）。
        events
                .keyBy(new KeySelector<String, String>() {
                    @Override
                    public String getKey(String value) {
                        return value.split(",")[0];
                    }
                })
                .process(new KeyedProcessFunction<String, String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) {
                        long ts = ctx.timestamp();
                        ctx.timerService().registerEventTimeTimer(ts);   // 等水位线越过 ts 时回调
                        out.collect(String.format("元素 %-4s eventTs=%-6d → 注册定时器（此刻 watermark=%d）",
                                value.split(",")[0], ts, ctx.timerService().currentWatermark()));
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) {
                        out.collect(String.format("定时器 @%-6d 触发 ⇒ 水位线已越过该时间戳，此刻 watermark=%d",
                                timestamp, ctx.timerService().currentWatermark()));
                    }
                })
                .name("A-watermark-observer")
                .print("L39-WATERMARK");

        // ---------- 支路B：事件时间窗口 + 自定义 Trigger ----------
        events.map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) {
                        return Tuple2.of(value.split(",")[0], 1);
                    }
                })
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .trigger(new LoggingTrigger())
                .process(new ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context ctx, Iterable<Tuple2<String, Integer>> elements,
                                        Collector<String> out) {
                        int sum = 0;
                        for (Tuple2<String, Integer> e : elements) {
                            sum += e.f1;
                        }
                        out.collect(String.format("窗口[%d, %d)  key=%-4s count=%d",
                                ctx.window().getStart(), ctx.window().getEnd(), key, sum));
                    }
                })
                .name("B-event-time-window")
                .print("L39-WINDOW");

        env.execute("L39 Watermark + Trigger Demo");
    }

    /** 自定义 Trigger：把回调打印出来，事件时间到点就 FIRE_AND_PURGE */
    public static class LoggingTrigger extends Trigger<Tuple2<String, Integer>, TimeWindow> {

        @Override
        public TriggerResult onElement(Tuple2<String, Integer> element, long timestamp,
                                       TimeWindow window, TriggerContext ctx) {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
            System.out.println(String.format(
                    "[TRIGGER] onEventTime(watermark=%d) 窗口[%d,%d) 到点 → 返回 FIRE_AND_PURGE",
                    time, window.getStart(), window.getEnd()));
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) {
            System.out.println(String.format("[TRIGGER] clear() 窗口[%d,%d) 状态已清理",
                    window.getStart(), window.getEnd()));
        }
    }
}
