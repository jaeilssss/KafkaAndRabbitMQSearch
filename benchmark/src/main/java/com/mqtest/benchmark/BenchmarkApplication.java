package com.mqtest.benchmark;

import com.mqtest.common.Scenario;
import com.mqtest.common.ScenarioLoader;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

/**
 * 사용법:
 * <pre>
 * java -jar benchmark.jar --scenario=scenarios/smoke-kafka.yml [--results-dir=results]
 * java -jar benchmark.jar --experiment=experiments/exp2-producer-scaling.yml --profile=quick
 *      [--brokers=kafka,rabbitmq] [--force] [--dry-run] [--prometheus-url=http://localhost:9090]
 * </pre>
 */
@SpringBootApplication(exclude = {
        org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration.class,
        org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration.class})
public class BenchmarkApplication {

    private static final Logger log = LoggerFactory.getLogger(BenchmarkApplication.class);

    public static void main(String[] args) {
        int exit = 0;
        try {
            SpringApplication app = new SpringApplication(BenchmarkApplication.class);
            app.setWebApplicationType(org.springframework.boot.WebApplicationType.NONE);
            app.setBannerMode(org.springframework.boot.Banner.Mode.OFF);
            app.run(args).close();
        } catch (Exception e) {
            log.error("benchmark failed", e);
            exit = 1;
        }
        System.exit(exit);
    }

    @Bean
    CommandLineRunner run(org.springframework.boot.ApplicationArguments args) {
        return ignored -> {
            String resultsDir = single(args, "results-dir");
            String experimentPath = single(args, "experiment");
            if (experimentPath != null) {
                runExperiment(args, experimentPath, Path.of(resultsDir == null ? "results" : resultsDir));
                return;
            }
            String scenarioPath = single(args, "scenario");
            if (scenarioPath == null) {
                throw new IllegalArgumentException("--scenario=<file.yml> or --experiment=<file.yml> is required");
            }
            Scenario scenario = ScenarioLoader.load(Path.of(scenarioPath));
            Path dir = Path.of(resultsDir == null ? "results" : resultsDir);
            var aggregates = new SuiteRunner(scenario, Environment.collect(), dir).run();
            for (var a : aggregates) {
                log.info("step rate={} runs={} consumer={} msg/s (stddev {}) achieved={} p99={}ms lostTotal={} generatorSaturated={}",
                        a.targetRatePerSec() == null ? "unlimited" : a.targetRatePerSec(), a.runs(),
                        Math.round(a.consumerMsgPerSec().mean()), Math.round(a.consumerMsgPerSec().stddev()),
                        a.achievedRatio() == null ? "-" : String.format("%.3f", a.achievedRatio()),
                        String.format("%.2f", a.p99Ms().mean()), a.lostTotal(), a.generatorSaturated());
            }
        };
    }

    private static void runExperiment(org.springframework.boot.ApplicationArguments args, String file, Path resultsDir)
            throws Exception {
        String profile = single(args, "profile");
        if (profile == null) {
            throw new IllegalArgumentException("--profile=<full|quick> is required with --experiment");
        }
        java.util.Set<com.mqtest.common.Broker> brokers = new java.util.LinkedHashSet<>();
        String brokerArg = single(args, "brokers");
        if (brokerArg != null) {
            for (String b : brokerArg.split(",")) {
                brokers.add(com.mqtest.common.Broker.valueOf(b.trim().toUpperCase()));
            }
        }
        String prometheus = single(args, "prometheus-url");
        ResourceCollector collector = new PrometheusClient(prometheus == null ? "http://localhost:9090" : prometheus,
                java.time.Duration.ofSeconds(6)); // scrape 간격(5s) 이후 반영 대기
        ExperimentRunner runner = new ExperimentRunner(Path.of(file), profile, brokers, resultsDir,
                args.containsOption("force"), collector);
        if (args.containsOption("dry-run")) {
            runner.dryRun();
        } else {
            runner.run();
        }
    }

    private static String single(org.springframework.boot.ApplicationArguments args, String name) {
        var values = args.getOptionValues(name);
        return values == null || values.isEmpty() ? null : values.get(0);
    }
}
