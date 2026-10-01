package com.mqtest.common;

import java.util.Arrays;
import java.util.List;
import java.util.function.ToDoubleFunction;

/** 같은 조건(step)의 반복 Run 들을 mean / median / 표본 표준편차로 집계한다. */
public final class Aggregator {

    private Aggregator() {
    }

    public record Stat(double mean, double median, double stddev) {

        public static Stat of(double[] values) {
            if (values.length == 0) {
                return new Stat(0, 0, 0);
            }
            double[] sorted = values.clone();
            Arrays.sort(sorted);
            double mean = Arrays.stream(sorted).average().orElse(0);
            int n = sorted.length;
            double median = n % 2 == 1 ? sorted[n / 2] : (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0;
            double variance = n < 2 ? 0 : Arrays.stream(sorted).map(v -> (v - mean) * (v - mean)).sum() / (n - 1);
            return new Stat(mean, median, Math.sqrt(variance));
        }
    }

    public record StepAggregate(
            Double targetRatePerSec,
            int runs,
            Stat producerMsgPerSec,
            Stat consumerMsgPerSec,
            Stat p50Ms,
            Stat p95Ms,
            Stat p99Ms,
            Stat lost,
            long lostTotal,
            Double achievedRatio,
            boolean generatorSaturated) {
    }

    /** @param targetRatePerSec rate 제어를 쓰지 않았으면 null */
    public static StepAggregate aggregate(Double targetRatePerSec, List<RunResult> runs) {
        Stat consumer = stat(runs, RunResult::consumerMsgPerSec);
        return new StepAggregate(
                targetRatePerSec,
                runs.size(),
                stat(runs, RunResult::producerMsgPerSec),
                consumer,
                stat(runs, r -> r.latency().p50Ms()),
                stat(runs, r -> r.latency().p95Ms()),
                stat(runs, r -> r.latency().p99Ms()),
                stat(runs, RunResult::lost),
                runs.stream().mapToLong(RunResult::lost).sum(),
                targetRatePerSec == null ? null : consumer.mean() / targetRatePerSec,
                runs.stream().anyMatch(RunResult::generatorSaturated));
    }

    private static Stat stat(List<RunResult> runs, ToDoubleFunction<RunResult> f) {
        return Stat.of(runs.stream().mapToDouble(f).toArray());
    }
}
