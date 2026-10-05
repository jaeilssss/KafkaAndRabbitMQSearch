package com.mqtest.common;

/** 실험의 한 포인트 = (broker, 변수값). 프로파일의 repetitions 만큼 Run 을 반복해 집계한다. */
public record ExperimentPoint(
        String experiment,
        String profile,
        Broker broker,
        String variable,
        String value,
        Scenario scenario) {

    /** 결과 디렉터리 이름. 예: {@code kafka__producers=4} */
    public String dirName() {
        return broker.name().toLowerCase() + "__" + variable + "=" + value;
    }
}
