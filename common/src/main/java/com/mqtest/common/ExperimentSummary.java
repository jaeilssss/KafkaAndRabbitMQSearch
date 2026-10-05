package com.mqtest.common;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/** 실험 전체 포인트를 한 표로 모은 CSV (그래프 입력). */
public final class ExperimentSummary {

    public record Row(ExperimentPoint point, Aggregator.StepAggregate aggregate) {
    }

    private static final String HEADER = "experiment,profile,broker,variable,value,runs,"
            + "producerMsgPerSec_mean,producerMsgPerSec_median,producerMsgPerSec_stddev,"
            + "consumerMsgPerSec_mean,consumerMsgPerSec_median,consumerMsgPerSec_stddev,consumerMBPerSec_mean,"
            + "p50Ms_mean,p95Ms_mean,p99Ms_mean,lostTotal,generatorSaturated,"
            + "cpuCoresAvg,cpuCoresMax,memoryMaxMB,netRxMBps,netTxMBps,diskWriteMBps";

    private ExperimentSummary() {
    }

    public static Path write(Path root, String experiment, String profile, List<Row> rows) throws IOException {
        Files.createDirectories(root);
        Path csv = root.resolve("experiment-summary.csv");
        List<String> lines = new ArrayList<>();
        lines.add(HEADER);
        for (Row r : rows) {
            lines.add(line(experiment, profile, r));
        }
        Files.write(csv, lines);
        return csv;
    }

    private static String line(String experiment, String profile, Row row) {
        ExperimentPoint p = row.point();
        Aggregator.StepAggregate a = row.aggregate();
        ResourceMetrics r = a.resources() == null ? ResourceMetrics.empty() : a.resources();
        return String.join(",",
                experiment, profile, p.broker().name(), p.variable(), p.value(), String.valueOf(a.runs()),
                f(a.producerMsgPerSec().mean()), f(a.producerMsgPerSec().median()), f(a.producerMsgPerSec().stddev()),
                f(a.consumerMsgPerSec().mean()), f(a.consumerMsgPerSec().median()), f(a.consumerMsgPerSec().stddev()),
                f(a.consumerMBPerSec().mean()),
                f(a.p50Ms().mean()), f(a.p95Ms().mean()), f(a.p99Ms().mean()),
                String.valueOf(a.lostTotal()), String.valueOf(a.generatorSaturated()),
                f(r.cpuCoresAvg()), f(r.cpuCoresMax()), f(mb(r.memoryMaxBytes())),
                f(mb(r.netRxBytesPerSec())), f(mb(r.netTxBytesPerSec())), f(mb(r.diskWriteBytesPerSec())));
    }

    private static Double mb(Double bytes) {
        return bytes == null ? null : bytes / 1_000_000.0;
    }

    private static String f(Double v) {
        return v == null ? "" : String.format(Locale.ROOT, "%.3f", v);
    }
}
