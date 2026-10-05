package com.mqtest.common;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/** Run 측정 구간 동안의 브로커 컨테이너 자원 사용량. 조회하지 못한 값은 null. */
public record ResourceMetrics(
        Double cpuCoresAvg,
        Double cpuCoresMax,
        Double memoryMaxBytes,
        Double netRxBytesPerSec,
        Double netTxBytesPerSec,
        Double diskWriteBytesPerSec) {

    public static ResourceMetrics empty() {
        return new ResourceMetrics(null, null, null, null, null, null);
    }

    /** 반복 Run 들의 자원 지표를 합친다: 평균류는 평균, 최대류는 최댓값. null 은 무시한다. */
    public static ResourceMetrics average(List<ResourceMetrics> list) {
        List<ResourceMetrics> present = list.stream().filter(Objects::nonNull).toList();
        return new ResourceMetrics(
                mean(present, ResourceMetrics::cpuCoresAvg),
                max(present, ResourceMetrics::cpuCoresMax),
                max(present, ResourceMetrics::memoryMaxBytes),
                mean(present, ResourceMetrics::netRxBytesPerSec),
                mean(present, ResourceMetrics::netTxBytesPerSec),
                mean(present, ResourceMetrics::diskWriteBytesPerSec));
    }

    private static Double mean(List<ResourceMetrics> list, Function<ResourceMetrics, Double> f) {
        var values = list.stream().map(f).filter(Objects::nonNull).mapToDouble(Double::doubleValue).toArray();
        return values.length == 0 ? null : java.util.Arrays.stream(values).average().getAsDouble();
    }

    private static Double max(List<ResourceMetrics> list, Function<ResourceMetrics, Double> f) {
        var values = list.stream().map(f).filter(Objects::nonNull).mapToDouble(Double::doubleValue).toArray();
        return values.length == 0 ? null : java.util.Arrays.stream(values).max().getAsDouble();
    }
}
