package com.mqtest.common;

import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

/** 스레드 안전한 latency 기록기. 내부 단위는 ns, 보고 단위는 ms. */
public final class LatencyStats {

    private static final long MAX_TRACKABLE_NANOS = TimeUnit.MINUTES.toNanos(10);

    private final Recorder recorder = new Recorder(MAX_TRACKABLE_NANOS, 3);

    public void recordNanos(long nanos) {
        recorder.recordValue(Math.min(Math.max(nanos, 1), MAX_TRACKABLE_NANOS));
    }

    public Percentiles snapshot() {
        Histogram h = recorder.getIntervalHistogram();
        return Percentiles.from(h);
    }

    public record Percentiles(long count, double meanMs, double p50Ms, double p95Ms, double p99Ms,
                              double p999Ms, double maxMs) {

        static Percentiles from(Histogram h) {
            if (h.getTotalCount() == 0) {
                return new Percentiles(0, 0, 0, 0, 0, 0, 0);
            }
            return new Percentiles(
                    h.getTotalCount(),
                    ms(h.getMean()),
                    ms(h.getValueAtPercentile(50)),
                    ms(h.getValueAtPercentile(95)),
                    ms(h.getValueAtPercentile(99)),
                    ms(h.getValueAtPercentile(99.9)),
                    ms(h.getMaxValue()));
        }

        private static double ms(double nanos) {
            return nanos / 1_000_000.0;
        }
    }
}
