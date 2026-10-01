package com.mqtest.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import org.junit.jupiter.api.Test;

class ScenarioLoaderTest {

    @Test
    void appliesDefaultsForOmittedFields() throws IOException {
        Scenario s = ScenarioLoader.parse("""
                name: smoke
                broker: kafka
                """);

        assertThat(s.broker()).isEqualTo(Broker.KAFKA);
        assertThat(s.messageSizeBytes()).isEqualTo(1024);
        assertThat(s.producers()).isEqualTo(1);
        assertThat(s.kafka().partitions()).isEqualTo(1);
        assertThat(s.kafka().acks()).isEqualTo("1");
        assertThat(s.rabbitmq().prefetch()).isEqualTo(250);
    }

    @Test
    void parsesBrokerSpecificOptions() throws IOException {
        Scenario s = ScenarioLoader.parse("""
                name: exp5
                broker: RabbitMQ
                messageSizeBytes: 100
                producers: 4
                consumers: 8
                warmupSeconds: 120
                measureSeconds: 300
                cooldownSeconds: 60
                rabbitmq:
                  queueType: quorum
                  publisherConfirms: true
                  prefetch: 50
                kafka:
                  partitions: 12
                  acks: all
                """);

        assertThat(s.broker()).isEqualTo(Broker.RABBITMQ);
        assertThat(s.warmupSeconds()).isEqualTo(120);
        assertThat(s.rabbitmq().queueType()).isEqualTo("quorum");
        assertThat(s.rabbitmq().publisherConfirms()).isTrue();
        assertThat(s.kafka().partitions()).isEqualTo(12);
    }

    @Test
    void rejectsInvalidScenarios() {
        assertThatThrownBy(() -> ScenarioLoader.parse("broker: kafka\n"))
                .hasRootCauseInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ScenarioLoader.parse("name: x\nbroker: kafka\nmessageSizeBytes: 4\n"))
                .hasRootCauseInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ScenarioLoader.parse("name: x\nbroker: kafka\nunknownField: 1\n"))
                .isInstanceOf(IOException.class);
    }

    @Test
    void parsesRateControlFieldsAndDefaults() throws IOException {
        Scenario plain = ScenarioLoader.parse("name: x\nbroker: kafka\n");
        assertThat(plain.targetRatePerSec()).isNull();
        assertThat(plain.rateSteps()).isNull();
        assertThat(plain.repetitions()).isEqualTo(1);
        assertThat(plain.pauseBetweenRunsSeconds()).isEqualTo(5);
        assertThat(plain.messageCount()).isEqualTo(10_000L);

        Scenario sweep = ScenarioLoader.parse("""
                name: sweep
                broker: kafka
                rateSteps: [2000, 4000]
                repetitions: 3
                pauseBetweenRunsSeconds: 1
                """);
        assertThat(sweep.rateSteps()).containsExactly(2000.0, 4000.0);
        assertThat(sweep.repetitions()).isEqualTo(3);
        assertThat(sweep.messageCount()).isEqualTo(Long.MAX_VALUE); // 시간 기반 종료

        Scenario fixed = ScenarioLoader.parse("name: x\nbroker: kafka\ntargetRatePerSec: 5000\nmessageCount: 77\n");
        assertThat(fixed.targetRatePerSec()).isEqualTo(5000.0);
        assertThat(fixed.messageCount()).isEqualTo(77L);
    }

    @Test
    void rejectsTargetRateTogetherWithRateSteps() {
        assertThatThrownBy(() -> ScenarioLoader.parse(
                "name: x\nbroker: kafka\ntargetRatePerSec: 1000\nrateSteps: [1000, 2000]\n"))
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage("targetRatePerSec and rateSteps cannot be used together");
        assertThatThrownBy(() -> ScenarioLoader.parse("name: x\nbroker: kafka\nrateSteps: []\n"))
                .hasRootCauseInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ScenarioLoader.parse("name: x\nbroker: kafka\nrepetitions: 0\n"))
                .hasRootCauseInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void withTargetRateReplacesStepsAndKeepsOtherFields() throws IOException {
        Scenario sweep = ScenarioLoader.parse("name: s\nbroker: rabbitmq\nrateSteps: [100, 200]\nproducers: 3\n");

        Scenario step = sweep.withTargetRate(200.0);

        assertThat(step.targetRatePerSec()).isEqualTo(200.0);
        assertThat(step.rateSteps()).isNull();
        assertThat(step.producers()).isEqualTo(3);
        assertThat(step.messageCount()).isEqualTo(Long.MAX_VALUE);
    }
}
