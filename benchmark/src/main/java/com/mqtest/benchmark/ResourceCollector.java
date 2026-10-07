package com.mqtest.benchmark;

import com.mqtest.common.Broker;
import com.mqtest.common.ResourceMetrics;
import java.nio.file.Path;
import java.time.Instant;

/** Run 측정 구간 동안의 브로커 컨테이너 자원 지표를 조회한다. 실패해도 예외 대신 null 필드를 돌려준다. */
interface ResourceCollector {

    ResourceMetrics collect(Broker broker, Instant start, Instant end);

    /** [start, end] 구간의 지표 시계열(자원 + consumer lag / queue depth)을 CSV 로 저장한다. 기본 구현은 아무것도 하지 않는다. */
    default void recordTimeSeries(Broker broker, Instant start, Instant end, Path file) {
    }
}
