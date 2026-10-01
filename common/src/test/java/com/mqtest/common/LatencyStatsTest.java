package com.mqtest.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class LatencyStatsTest {

    @Test
    void computesPercentilesInMilliseconds() {
        LatencyStats stats = new LatencyStats();
        for (int ms = 1; ms <= 1000; ms++) {
            stats.recordNanos(TimeUnit.MILLISECONDS.toNanos(ms));
        }

        LatencyStats.Percentiles p = stats.snapshot();

        assertThat(p.count()).isEqualTo(1000);
        assertThat(p.p50Ms()).isCloseTo(500, within(1.0));
        assertThat(p.p95Ms()).isCloseTo(950, within(2.0));
        assertThat(p.p99Ms()).isCloseTo(990, within(2.0));
        assertThat(p.p999Ms()).isCloseTo(999, within(2.0));
        assertThat(p.maxMs()).isCloseTo(1000, within(1.0));
    }

    @Test
    void emptySnapshotIsAllZero() {
        LatencyStats.Percentiles p = new LatencyStats().snapshot();

        assertThat(p.count()).isZero();
        assertThat(p.p99Ms()).isZero();
    }
}
