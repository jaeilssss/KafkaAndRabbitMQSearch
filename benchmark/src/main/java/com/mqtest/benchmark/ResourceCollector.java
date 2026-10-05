package com.mqtest.benchmark;

import com.mqtest.common.Broker;
import com.mqtest.common.ResourceMetrics;
import java.time.Instant;

/** Run 측정 구간 동안의 브로커 컨테이너 자원 지표를 조회한다. 실패해도 예외 대신 null 필드를 돌려준다. */
interface ResourceCollector {

    ResourceMetrics collect(Broker broker, Instant start, Instant end);
}
