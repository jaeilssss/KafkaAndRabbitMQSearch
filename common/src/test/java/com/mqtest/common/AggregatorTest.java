package com.mqtest.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AggregatorTest {

    @Test
    void statComputesMeanMedianAndSampleStdDev() {
        Aggregator.Stat s = Aggregator.Stat.of(new double[] {1, 2, 3, 4, 10});

        assertThat(s.mean()).isCloseTo(4.0, within(1e-9));
        assertThat(s.median()).isCloseTo(3.0, within(1e-9));
        assertThat(s.stddev()).isCloseTo(3.5355339, within(1e-6)); // sqrt(50 / 4)
    }

    @Test
    void medianOfEvenCountAveragesMiddleTwoAndSingleSampleHasZeroStdDev() {
        assertThat(Aggregator.Stat.of(new double[] {4, 1, 3, 2}).median()).isCloseTo(2.5, within(1e-9));
        assertThat(Aggregator.Stat.of(new double[] {7}).stddev()).isZero();
    }

    @Test
    void aggregatesRunsOfOneStep() {
        List<RunResult> runs = List.of(
                run(1000, 990, 5, 0, 0.30, false),
                run(1000, 1010, 7, 0, 0.40, false),
                run(1000, 1000, 9, 2, 0.50, true));

        Aggregator.StepAggregate a = Aggregator.aggregate(1000.0, runs);

        assertThat(a.runs()).isEqualTo(3);
        assertThat(a.targetRatePerSec()).isEqualTo(1000.0);
        assertThat(a.consumerMsgPerSec().mean()).isCloseTo(1000.0, within(1e-9));
        assertThat(a.consumerMsgPerSec().stddev()).isCloseTo(10.0, within(1e-9));
        assertThat(a.p99Ms().median()).isCloseTo(7.0, within(1e-9));
        assertThat(a.lostTotal()).isEqualTo(2);
        assertThat(a.achievedRatio()).isCloseTo(1.0, within(1e-9));
        assertThat(a.generatorSaturated()).isTrue();
    }

    @Test
    void achievedRatioIsNullWithoutTargetRate() {
        Aggregator.StepAggregate a = Aggregator.aggregate(null, List.of(run(0, 500, 5, 0, 0.1, false)));

        assertThat(a.achievedRatio()).isNull();
    }

    @Test
    void generatorSaturatedWhenLagOrCpuExceedsThreshold() {
        assertThat(GeneratorHealth.isSaturated(10.1, 0.1)).isTrue();
        assertThat(GeneratorHealth.isSaturated(1.0, 0.81)).isTrue();
        assertThat(GeneratorHealth.isSaturated(10.0, 0.80)).isFalse();
    }

    @Test
    void writesAggregateAndSweepCsv(@TempDir Path dir) throws IOException {
        Aggregator.StepAggregate s1 = Aggregator.aggregate(1000.0, List.of(run(1000, 998, 5, 0, 0.1, false)));
        Aggregator.StepAggregate s2 = Aggregator.aggregate(2000.0, List.of(run(2000, 1500, 90, 3, 0.9, true)));

        AggregateWriter.Paths paths = AggregateWriter.write(dir, "exp", "20261001T000000Z", List.of(s1, s2), true);

        List<String> sweep = Files.readAllLines(paths.sweepCsv());
        assertThat(sweep.get(0)).isEqualTo("targetRatePerSec,runs,consumerMsgPerSec,achievedRatio,p99Ms,lostTotal,generatorSaturated");
        assertThat(sweep).hasSize(3);
        assertThat(sweep.get(2)).startsWith("2000.0,1,1500.00,0.7500,90.000,3,true");
        List<String> agg = Files.readAllLines(paths.aggregateCsv());
        assertThat(agg.get(0)).contains("consumerMsgPerSec_mean").contains("consumerMsgPerSec_median").contains("consumerMsgPerSec_stddev");
        assertThat(agg).hasSize(3);
        assertThat(Files.readString(paths.aggregateJson())).contains("\"targetRatePerSec\"");
    }

    @Test
    void sweepCsvIsOmittedWhenNotASweep(@TempDir Path dir) throws IOException {
        AggregateWriter.Paths paths = AggregateWriter.write(dir, "exp", "t",
                List.of(Aggregator.aggregate(null, List.of(run(0, 10, 1, 0, 0.1, false)))), false);

        assertThat(paths.sweepCsv()).isNull();
        assertThat(Files.exists(paths.aggregateCsv())).isTrue();
    }

    private static RunResult run(double target, double consumerRate, double p99, long lost, double cpu, boolean saturated) {
        LatencyStats.Percentiles lat = new LatencyStats.Percentiles(100, p99 / 2, p99 / 3, p99 / 2, p99, p99, p99);
        return new RunResult("exp", Broker.KAFKA, "t", 1024, 1, 1, 100, 100 - lost, lost, 100, 100, 1.0,
                consumerRate, consumerRate, 0, 0, lat, null, Map.of(),
                target == 0 ? null : target, 1.0, cpu, saturated, null);
    }
}
