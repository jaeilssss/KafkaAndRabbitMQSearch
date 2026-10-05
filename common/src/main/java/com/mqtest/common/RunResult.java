package com.mqtest.common;

import java.util.Map;

/** Run 1회의 결과. JSON 으로 직렬화되어 results/ 에 저장된다. */
public record RunResult(
        String scenario,
        Broker broker,
        String startedAt,
        int messageSizeBytes,
        int producers,
        int consumers,
        long sent,
        long received,
        long lost,
        long measuredSent,
        long measuredReceived,
        double measureWindowSeconds,
        double producerMsgPerSec,
        double consumerMsgPerSec,
        double producerMBPerSec,
        double consumerMBPerSec,
        LatencyStats.Percentiles latency,
        Scenario config,
        Map<String, String> environment,
        Double targetRatePerSec,
        double scheduleLagP99Ms,
        double generatorProcessCpuLoad,
        boolean generatorSaturated,
        ResourceMetrics resources) {
}
