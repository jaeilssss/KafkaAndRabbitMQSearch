package com.mqtest.benchmark;

import com.mqtest.common.AggregateWriter;
import com.mqtest.common.Aggregator;
import com.mqtest.common.ResultWriter;
import com.mqtest.common.RunResult;
import com.mqtest.common.Scenario;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * rateSteps × repetitions 를 순서대로 실행하고 step 별로 집계한다.
 * rate 옵션이 없으면 (무제한 모드, 기존 동작) 1 step 이다.
 */
final class SuiteRunner {

    private static final Logger log = LoggerFactory.getLogger(SuiteRunner.class);

    private final Scenario scenario;
    private final Map<String, String> environment;
    private final Path resultsDir;
    private final ResourceCollector collector;

    SuiteRunner(Scenario scenario, Map<String, String> environment, Path resultsDir) {
        this(scenario, environment, resultsDir, null);
    }

    SuiteRunner(Scenario scenario, Map<String, String> environment, Path resultsDir, ResourceCollector collector) {
        this.collector = collector;
        this.scenario = scenario;
        this.environment = environment;
        this.resultsDir = resultsDir;
    }

    List<Aggregator.StepAggregate> run() throws Exception {
        boolean sweep = scenario.rateSteps() != null;
        List<Double> steps = new ArrayList<>();
        if (sweep) {
            steps.addAll(scenario.rateSteps());
        } else {
            steps.add(scenario.targetRatePerSec()); // null = 무제한
        }

        String stamp = Instant.now().toString().replace(":", "").replace(".", "");
        List<Aggregator.StepAggregate> aggregates = new ArrayList<>();
        boolean first = true;
        for (Double rate : steps) {
            Scenario stepScenario = sweep ? scenario.withTargetRate(rate) : scenario;
            List<RunResult> runs = new ArrayList<>();
            for (int rep = 1; rep <= scenario.repetitions(); rep++) {
                if (!first) {
                    Thread.sleep(scenario.pauseBetweenRunsSeconds() * 1000L);
                }
                first = false;
                log.info("step rate={} repetition {}/{}", rate == null ? "unlimited" : rate, rep, scenario.repetitions());
                RunResult result = new ScenarioRunner(stepScenario, environment, collector).run();
                Path out = ResultWriter.write(resultsDir, result);
                log.info("run result: {}", out);
                runs.add(result);
            }
            aggregates.add(Aggregator.aggregate(rate, runs));
        }

        AggregateWriter.Paths paths = AggregateWriter.write(resultsDir, scenario.name(), stamp, aggregates, sweep);
        log.info("aggregate: {}", paths.aggregateCsv());
        if (paths.sweepCsv() != null) {
            log.info("sweep: {}", paths.sweepCsv());
        }
        return aggregates;
    }
}
