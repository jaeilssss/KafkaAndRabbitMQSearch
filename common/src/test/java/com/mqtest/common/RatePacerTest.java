package com.mqtest.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import java.util.List;
import org.junit.jupiter.api.Test;

class RatePacerTest {

    private static final long SECOND = 1_000_000_000L;

    @Test
    void splitsTotalRateEvenlyAcrossProducers() {
        List<RatePacer> pacers = RatePacer.split(10_000, 4, 0);

        assertThat(pacers).hasSize(4);
        // 각 producer 는 2,500 msg/s => 간격 400us, 시작 위상은 100us 씩 어긋난다.
        assertThat(pacers.get(0).scheduledAt(1) - pacers.get(0).scheduledAt(0)).isEqualTo(400_000L);
        assertThat(pacers.get(2).scheduledAt(0)).isEqualTo(200_000L);
    }

    @Test
    void cumulativeCountTracksTargetWithinOnePercent() {
        for (int producers : new int[] {1, 3, 8}) {
            List<RatePacer> pacers = RatePacer.split(5_000, producers, 0);
            for (long seconds : new long[] {1, 5, 20}) {
                long due = pacers.stream().mapToLong(p -> p.countDueBy(seconds * SECOND)).sum();
                assertThat((double) due).isCloseTo(5_000.0 * seconds, within(5_000.0 * seconds * 0.01));
            }
        }
    }

    @Test
    void scheduleIsMonotonicAndAnchoredAtStart() {
        RatePacer pacer = RatePacer.split(1_000, 1, 7_000L).get(0);

        assertThat(pacer.scheduledAt(0)).isEqualTo(7_000L);
        assertThat(pacer.scheduledAt(10)).isGreaterThan(pacer.scheduledAt(9));
        assertThat(pacer.countDueBy(6_999L)).isZero();
    }

    @Test
    void rejectsNonPositiveRate() {
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> RatePacer.split(0, 1, 0));
    }
}
