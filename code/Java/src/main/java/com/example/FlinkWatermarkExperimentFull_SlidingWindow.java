package com.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;

/**
 * Flink 1.9.3 实验：多组 Watermark delay 对比
 *
 * 使用滑动窗口：窗口长度 5 秒，滑动步长 1 秒
 *
 * 输出：
 *  - detail_<delay>.csv   : 事件级明细（用于统计 dropped / trigger delay / accuracy）
 *  - summary_<delay>.csv  : 窗口级汇总（每个窗口一行）
 *
 * CSV 说明见注释。
 */
public class FlinkWatermarkExperimentFull_SlidingWindow {

    public static void main(String[] args) throws Exception {

        final String socketHost = "192.168.1.110";
        final int socketPort = 9999;
        final String outBase = "D:/flink_csv/SlidingWindow/";

        // Watermark delays
        final long[] delays = new long[]{0L, 2000L, 4000L, 5000L, 8000L, 10000L, 15000L, 20000L};

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(3);
        env.getConfig().setAutoWatermarkInterval(200);

        DataStream<String> raw = env.socketTextStream(socketHost, socketPort);

        DataStream<Tuple3<String, Long, Double>> source = raw
                .map(new MapFunction<String, Tuple3<String, Long, Double>>() {
                    @Override
                    public Tuple3<String, Long, Double> map(String v) throws Exception {
                        try {
                            String[] p = v.split(",");
                            return new Tuple3<>(p[0], Long.parseLong(p[1]), Double.parseDouble(p[2]));
                        } catch (Exception ex) {
                            return null;
                        }
                    }
                })
                .filter(x -> x != null);

        for (long delay : delays) {
            String label = "delay" + delay;
            runDelayGroup(source, delay, label,
                    outBase + "detail_" + delay + ".csv",
                    outBase + "summary_" + delay + ".csv");
        }

        env.execute("Flink Watermark Multi-Delay Experiment (Sliding Window)");
    }


    /** 每个 delay 创建一个分支 */
    public static void runDelayGroup(
            DataStream<Tuple3<String, Long, Double>> source,
            final long delayMs,
            final String label,
            final String detailPath,
            final String summaryPath
    ) {

        final OutputTag<Tuple3<String, Long, Double>> droppedTag =
                new OutputTag<Tuple3<String, Long, Double>>("dropped-" + label) {};

        final OutputTag<String> detailTag =
                new OutputTag<String>("detail-" + label) {};

        SingleOutputStreamOperator<String> windowResults = source
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<String, Long, Double>>() {
                    private long currentMax = Long.MIN_VALUE;

                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        if (currentMax == Long.MIN_VALUE) {
                            return new Watermark(Long.MIN_VALUE);
                        } else {
                            return new Watermark(currentMax - delayMs);
                        }
                    }

                    @Override
                    public long extractTimestamp(Tuple3<String, Long, Double> e, long prev) {
                        long ts = e.f1;
                        currentMax = Math.max(currentMax, ts);
                        return ts;
                    }
                })

                .keyBy(0)

                // -------------------------------------------
                // ✅ 改为滑动窗口：窗口长度 5 秒 + 滑动步长 1 秒
                // -------------------------------------------
                .window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1)))

                .sideOutputLateData(droppedTag)

                .process(new ProcessWindowFunction<Tuple3<String, Long, Double>, String,
                        org.apache.flink.api.java.tuple.Tuple, TimeWindow>() {

                    @Override
                    public void process(org.apache.flink.api.java.tuple.Tuple key,
                                        Context ctx,
                                        Iterable<Tuple3<String, Long, Double>> elements,
                                        Collector<String> out) {

                        long ws = ctx.window().getStart();
                        long we = ctx.window().getEnd();

                        long triggerTime = System.currentTimeMillis();
                        long windowLag = triggerTime - we;

                        int cnt = 0;
                        double sum = 0;

                        for (Tuple3<String, Long, Double> e : elements) {
                            cnt++;
                            sum += e.f2;
                        }

                        double avg = cnt == 0 ? 0.0 : sum / cnt;

                        // summary
                        String summary = String.format(
                                "%d,%d,%d,%d,%.6f,%d,%d",
                                delayMs, ws, we, cnt, avg, triggerTime, windowLag
                        );
                        out.collect(summary);

                        // detail
                        for (Tuple3<String, Long, Double> e : elements) {
                            long pt = System.currentTimeMillis();
                            String detailLine = String.format(
                                    "%d,%d,%.6f,%d,%d,%d,%d,%d,%.6f,%d,%d",
                                    e.f1, pt, e.f2,
                                    delayMs,
                                    ws, we,
                                    triggerTime,
                                    windowLag,
                                    avg,
                                    0, // late_flag
                                    0  // dropped_flag
                            );
                            ctx.output(detailTag, detailLine);
                        }
                    }
                });

        windowResults.writeAsText(summaryPath, FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        DataStream<String> droppedStream = windowResults.getSideOutput(droppedTag)
                .map(new MapFunction<Tuple3<String, Long, Double>, String>() {
                    @Override
                    public String map(Tuple3<String, Long, Double> e) throws Exception {
                        long arrival = System.currentTimeMillis();
                        return String.format(
                                "%d,%d,%.6f,%d,%d,%d,%d,%d,,%d,%d",
                                e.f1, arrival, e.f2,
                                delayMs,
                                -1L, -1L,
                                -1L, -1L,
                                0,
                                1 // dropped_flag
                        );
                    }
                });

        windowResults.getSideOutput(detailTag)
                .union(droppedStream)
                .writeAsText(detailPath, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);
    }
}
