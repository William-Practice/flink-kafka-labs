package com.example;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * L36 实验：Flink 并行度（优先级：算子级 > 提交级 -p > 环境级 env.setParallelism > 配置文件）
 *
 * 作业结构：
 *   source  fromElements(...)        → 非并行 Source，固定 (1/1)（实测：与 -p 无关！）
 *   stage0  不设并行度               → 跟随环境默认，-p 1 时 (1/1)、-p 4 时 (4/4)   ← 拉开差异的关键
 *   stage1  setParallelism(2)        → 永远 (2/2)
 *   stage2  setParallelism(3)        → 永远 (3/3)
 *   sink    print().setParallelism(1) → (1/1)
 *
 * 用 `-p 1` 和 `-p 4` 各跑一次，对比 JM 日志里各算子的 (n/n) 与 TM stdout 里的 subtask 编号。
 */
public class L36ParallelismDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        System.out.println("========================================");
        System.out.println("环境默认并行度 env.getParallelism()  = " + env.getParallelism());
        System.out.println("最大并行度   env.getMaxParallelism() = " + env.getMaxParallelism());
        System.out.println("========================================");

        // Source：fromElements 是非并行 source，并行度恒为 1
        DataStream<String> source = env.fromElements(
                "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l");

        // stage0：不设并行度 → 跟随环境默认（-p 的值）
        DataStream<String> stage0 = source
                .map(new RichMapFunction<String, String>() {
                    @Override
                    public String map(String value) {
                        return value + " | stage0(subtask#" + getRuntimeContext().getIndexOfThisSubtask() + ")";
                    }
                })
                .name("stage0-env-default");

        // stage1：算子级 2
        DataStream<String> stage1 = stage0
                .map(new RichMapFunction<String, String>() {
                    @Override
                    public String map(String value) {
                        return value + " | stage1(subtask#" + getRuntimeContext().getIndexOfThisSubtask() + ")";
                    }
                })
                .name("stage1-p2")
                .setParallelism(2);

        // stage2：算子级 3
        DataStream<String> stage2 = stage1
                .map(new RichMapFunction<String, String>() {
                    @Override
                    public String map(String value) {
                        return value + " | stage2(subtask#" + getRuntimeContext().getIndexOfThisSubtask() + ")";
                    }
                })
                .name("stage2-p3")
                .setParallelism(3);

        stage2.print("L36-RESULT").setParallelism(1);

        env.execute("L36 Parallelism Demo");
    }
}
