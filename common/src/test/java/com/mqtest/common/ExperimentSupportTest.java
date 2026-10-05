package com.mqtest.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ExperimentSupportTest {

    private static List<ExperimentPoint> exp2Points(String profile) throws IOException {
        return ExperimentDefinition.parse("""
                experiment: e2
                brokers: [kafka, rabbitmq]
                base: { consumers: 1, messageCount: 2147483647 }
                vary: { producers: [1, 2, 4, 8, 16, 32] }
                profiles:
                  full:  { warmupSeconds: 120, measureSeconds: 300, cooldownSeconds: 60, repetitions: 3, pauseBetweenRunsSeconds: 10 }
                  quick: { warmupSeconds: 5, measureSeconds: 15, cooldownSeconds: 5, repetitions: 1, pauseBetweenRunsSeconds: 3 }
                """).expand(profile, Set.of());
    }

    @Test
    void estimatesUpperBoundOfRunTime() throws IOException {
        long full = ExperimentPlanner.estimateSeconds(exp2Points("full"));
        long quick = ExperimentPlanner.estimateSeconds(exp2Points("quick"));

        assertThat(ExperimentPlanner.runCount(exp2Points("full"))).isEqualTo(36);
        assertThat(full).isGreaterThanOrEqualTo(12L * 3 * (120 + 300 + 60));
        assertThat(full).isLessThan(12L * 3 * (120 + 300 + 60) * 2);
        assertThat(quick).isLessThan(full / 10);
    }

    @Test
    void pointStoreTracksDoneMarkerAndRoundTripsAggregate(@TempDir Path root) throws IOException {
        ExperimentPoint p = exp2Points("quick").get(0);
        Path dir = PointStore.dir(root, p);

        assertThat(PointStore.isDone(dir)).isFalse();

        Aggregator.StepAggregate agg = aggregate(1234.5, 7.0);
        PointStore.writeAggregate(dir, agg);
        assertThat(PointStore.isDone(dir)).isFalse(); // DONE 은 집계 저장 후 명시적으로 표시
        PointStore.markDone(dir);

        assertThat(PointStore.isDone(dir)).isTrue();
        Aggregator.StepAggregate back = PointStore.readAggregate(dir);
        assertThat(back.consumerMsgPerSec().mean()).isCloseTo(1234.5, within(1e-9));
        assertThat(back.resources().cpuCoresAvg()).isCloseTo(1.5, within(1e-9));

        PointStore.clear(dir);
        assertThat(Files.exists(dir)).isFalse();
    }

    @Test
    void skipsDonePointsUnlessForced(@TempDir Path root) throws IOException {
        List<ExperimentPoint> points = exp2Points("quick");
        PointStore.writeAggregate(PointStore.dir(root, points.get(0)), aggregate(1, 1));
        PointStore.markDone(PointStore.dir(root, points.get(0)));

        assertThat(ExperimentPlanner.pending(root, points, false)).hasSize(11).doesNotContain(points.get(0));
        assertThat(ExperimentPlanner.pending(root, points, true)).hasSize(12);
    }

    @Test
    void summaryCsvHasOneRowPerPointWithResources(@TempDir Path root) throws IOException {
        List<ExperimentPoint> points = exp2Points("quick");
        List<ExperimentSummary.Row> rows = List.of(
                new ExperimentSummary.Row(points.get(0), aggregate(1000, 5)),
                new ExperimentSummary.Row(points.get(1), aggregate(1800, 9)));

        Path csv = ExperimentSummary.write(root, "e2", "quick", rows);

        List<String> lines = Files.readAllLines(csv);
        assertThat(lines).hasSize(3);
        assertThat(lines.get(0)).startsWith("experiment,profile,broker,variable,value,runs")
                .contains("consumerMsgPerSec_stddev", "generatorSaturated", "cpuCoresAvg", "memoryMaxMB", "netRxMBps", "diskWriteMBps");
        assertThat(lines.get(1)).startsWith("e2,quick,KAFKA,producers,1,1,");
        assertThat(lines.get(2)).startsWith("e2,quick,KAFKA,producers,2,1,");
    }

    static Aggregator.StepAggregate aggregate(double consumerRate, double p99) {
        LatencyStats.Percentiles lat = new LatencyStats.Percentiles(10, p99 / 2, p99 / 3, p99 / 2, p99, p99, p99);
        ResourceMetrics res = new ResourceMetrics(1.5, 2.0, 5.0e8, 1.0e6, 2.0e6, null);
        RunResult r = new RunResult("e2", Broker.KAFKA, "t", 1024, 1, 1, 10, 10, 0, 10, 10, 1.0,
                consumerRate, consumerRate, 0, consumerRate * 1024 / 1e6, lat, null, Map.of(), null, 0.0, 0.1, false, res);
        return Aggregator.aggregate(null, List.of(r));
    }
}
