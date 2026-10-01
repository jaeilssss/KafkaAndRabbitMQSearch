package com.mqtest.common;

import java.util.ArrayList;
import java.util.List;

/**
 * producer 1개의 발행 계획. n 번째 메시지의 계획 시각 = start + offset + n * interval.
 * 여러 producer 는 위상을 조금씩 어긋나게 해 같은 순간에 몰려서 보내지 않도록 한다.
 */
public final class RatePacer {

    private final long startNanos;
    private final double intervalNanos;

    private RatePacer(long startNanos, double intervalNanos) {
        this.startNanos = startNanos;
        this.intervalNanos = intervalNanos;
    }

    /** 전체 rate 를 producers 개로 균등 분할한다. producer i 의 위상 오프셋은 i / totalRate 초. */
    public static List<RatePacer> split(double totalRatePerSec, int producers, long startNanos) {
        if (totalRatePerSec <= 0 || producers <= 0) {
            throw new IllegalArgumentException("rate and producers must be > 0");
        }
        double perProducerInterval = 1e9 * producers / totalRatePerSec;
        double phase = 1e9 / totalRatePerSec;
        List<RatePacer> pacers = new ArrayList<>(producers);
        for (int i = 0; i < producers; i++) {
            pacers.add(new RatePacer(startNanos + Math.round(i * phase), perProducerInterval));
        }
        return pacers;
    }

    public long scheduledAt(long index) {
        return startNanos + Math.round(index * intervalNanos);
    }

    /** 계획 시각이 {@code nanos} 이하인 메시지 수. */
    public long countDueBy(long nanos) {
        if (nanos < startNanos) {
            return 0;
        }
        return (long) Math.floor((nanos - startNanos) / intervalNanos) + 1;
    }
}
