package com.mqtest.common;

/** 부하 생성기(러너 JVM) 포화 판정. 포화되면 측정한 처리량/지연이 브로커 한계가 아닐 수 있다. */
public final class GeneratorHealth {

    public static final double MAX_SCHEDULE_LAG_P99_MS = 10.0;
    public static final double MAX_PROCESS_CPU_LOAD = 0.80;

    private GeneratorHealth() {
    }

    public static boolean isSaturated(double scheduleLagP99Ms, double processCpuLoad) {
        return scheduleLagP99Ms > MAX_SCHEDULE_LAG_P99_MS || processCpuLoad > MAX_PROCESS_CPU_LOAD;
    }
}
