package com.mqtest.common;

import java.nio.file.Path;
import java.util.List;

public final class ExperimentPlanner {

    /** Run 1회당 고정 오버헤드(consumer 안정화, topology 생성/삭제, 자원 지표 settle 등) 추정치. */
    static final int RUN_OVERHEAD_SECONDS = 15;

    private ExperimentPlanner() {
    }

    public static long runCount(List<ExperimentPoint> points) {
        return points.stream().mapToLong(p -> p.scenario().repetitions()).sum();
    }

    /**
     * 실행 시간 추정(초). 개수 기반 종료(Exp 1)는 실제로 더 짧을 수 있고, 반대로 발행이 소비보다 훨씬 빠른 과부하 포인트는
     * backlog drain 때문에 더 길어질 수 있다(cooldown 은 소비 정체 허용 시간).
     */
    public static long estimateSeconds(List<ExperimentPoint> points) {
        long total = 0;
        for (ExperimentPoint p : points) {
            Scenario s = p.scenario();
            long perRun = s.warmupSeconds() + s.measureSeconds() + s.cooldownSeconds() + RUN_OVERHEAD_SECONDS;
            total += s.repetitions() * perRun + (long) (s.repetitions() - 1) * s.pauseBetweenRunsSeconds();
        }
        return total;
    }

    /** DONE 인 포인트를 건너뛴 실행 대상(force 면 전부). */
    public static List<ExperimentPoint> pending(Path root, List<ExperimentPoint> points, boolean force) {
        return force ? points : points.stream().filter(p -> !PointStore.isDone(PointStore.dir(root, p))).toList();
    }
}
