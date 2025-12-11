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
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Flink 1.9.3 实验：多组 Watermark 延迟对比（0, 2, 4, 5, 8 秒）
 *
 * 输出（每个 delay 分组）：
 * - detail_<delay>.csv   : 事件级别明细（用于统计 dropped / trigger delay / accuracy）
 * - summary_<delay>.csv  : 窗口级汇总（每个窗口一行）
 */
public class FlinkWatermarkExperimentFull {

    public static void main(String[] args) throws Exception {
        // 配置：按需修改 socket 地址与输出目录
        final String socketHost = "192.168.1.110";
        final int socketPort = 9999;
        final String outBase = "D:/flink_csv/RollingWindow/"; // 输出目录（保证有写权限）

        // Watermark delays (毫秒) - 修改为 0, 2, 4, 5, 8 ,10,15,20秒
        final long[] delays = new long[] {0L, 2000L, 4000L, 5000L, 8000L, 10000L, 15000L, 20000L};

        // Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(3);
        env.getConfig().setAutoWatermarkInterval(200L);

        // socket 输入，每行格式: id,eventTs_ms,value
        DataStream<String> raw = env.socketTextStream(socketHost, socketPort);

        // 转换成 Tuple3<id, eventTs(ms), value>
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

        // 为每个 watermark 延迟创建一个分支
        for (final long delay : delays) {
            final String label = "delay" + delay;
            runDelayGroup(source, delay, label,
                    outBase + "detail_" + delay + ".csv",
                    outBase + "summary_" + delay + ".csv");
        }

        env.execute("Flink Watermark Multi-Delay Experiment_RollingWindows");
    }

    /**
     * 为某个 delay 创建独立实验分支
     */
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
                    public long extractTimestamp(Tuple3<String, Long, Double> element, long previousElementTimestamp) {
                        long ts = element.f1;
                        currentMax = Math.max(currentMax, ts);
                        return ts;
                    }
                })
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sideOutputLateData(droppedTag)
                .process(new ProcessWindowFunction<Tuple3<String, Long, Double>, String, org.apache.flink.api.java.tuple.Tuple, TimeWindow>() {
                    @Override
                    public void process(org.apache.flink.api.java.tuple.Tuple key,
                                        Context ctx,
                                        Iterable<Tuple3<String, Long, Double>> elements,
                                        Collector<String> out) {

                        long windowStart = ctx.window().getStart();
                        long windowEnd = ctx.window().getEnd();
                        long triggerTime = System.currentTimeMillis();

                        int cnt = 0;
                        double sum = 0.0;

                        for (Tuple3<String, Long, Double> e : elements) {
                            cnt++;
                            sum += e.f2;
                        }

                        double avg = cnt == 0 ? 0.0 : sum / cnt;
                        long windowLag = triggerTime - windowEnd;

                        // 输出 summary CSV
                        // 格式: delay, win_start, win_end, count, avg, trigger_time, lag
                        String summary = String.format("%d,%d,%d,%d,%.6f,%d,%d",
                                delayMs,
                                windowStart,
                                windowEnd,
                                cnt,
                                avg,
                                triggerTime,
                                windowLag
                        );
                        out.collect(summary);

                        // 输出 detail CSV
                        // 格式: event_time, proc_time, value, delay, win_start, win_end, trig_time, lag, avg, late_flag, dropped_flag
                        for (Tuple3<String, Long, Double> e : elements) {
                            long processingTime = System.currentTimeMillis();
                            String perEvent = String.format("%d,%d,%.6f,%d,%d,%d,%d,%d,%.6f,%d,%d",
                                    e.f1,
                                    processingTime,
                                    e.f2,
                                    delayMs,
                                    windowStart,
                                    windowEnd,
                                    triggerTime,
                                    (triggerTime - windowEnd),
                                    avg,
                                    0,      // late_flag (in window)
                                    0       // dropped_flag
                            );
                            ctx.output(detailTag, perEvent);
                        }
                    }
                });

        // 写 summary CSV
        windowResults
                .writeAsText(summaryPath, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        // 处理被丢弃的迟到事件
        DataStream<String> droppedStream = windowResults.getSideOutput(droppedTag)
                .map(new MapFunction<Tuple3<String, Long, Double>, String>() {
                    @Override
                    public String map(Tuple3<String, Long, Double> e) throws Exception {
                        long arrival = System.currentTimeMillis();
                        // 格式保持与 detail 一致，但窗口相关字段填 -1 或空
                        return String.format("%d,%d,%.6f,%d,%d,%d,%d,%d,,%d,%d",
                                e.f1,
                                arrival,
                                e.f2,
                                delayMs,
                                -1L, -1L, -1L, -1L, // 窗口字段 -1
                                // 注意这里是双逗号 ,, 表示 avg 字段为空
                                1,                  // late_flag = 1
                                1                   // dropped_flag = 1
                        );
                    }
                });

        // 合并 detail + dropped，写 detail CSV
        windowResults.getSideOutput(detailTag)
                .union(droppedStream)
                .writeAsText(detailPath, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);
    }
}