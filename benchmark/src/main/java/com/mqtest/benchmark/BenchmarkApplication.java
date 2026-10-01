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
 * 사용법: {@code java -jar benchmark.jar --scenario=scenarios/smoke-kafka.yml [--results-dir=results]}
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
            String scenarioPath = single(args, "scenario");
            if (scenarioPath == null) {
                throw new IllegalArgumentException("--scenario=<file.yml> is required");
            }
            String resultsDir = single(args, "results-dir");
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

    private static String single(org.springframework.boot.ApplicationArguments args, String name) {
        var values = args.getOptionValues(name);
        return values == null || values.isEmpty() ? null : values.get(0);
    }
}
