package com.mqtest.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ExperimentDefinitionTest {

    private static final String EXP1 = """
            experiment: exp1-baseline
            brokers: [kafka, rabbitmq]
            base:
              messageSizeBytes: 1024
              producers: 1
              consumers: 1
              kafka: { partitions: 1 }
            vary:
              messageCount: [10000, 100000, 1000000]
            profiles:
              full:  { measureSeconds: 600, cooldownSeconds: 60, repetitions: 3, warmupMessagesPercent: 10, warmupMessagesMin: 1000 }
              quick: { measureSeconds: 600, cooldownSeconds: 5, repetitions: 1, warmupMessages: 1000 }
            """;

    private static final String EXP2 = """
            experiment: exp2-producer-scaling
            brokers: [kafka, rabbitmq]
            base:
              messageSizeBytes: 1024
              consumers: 1
              messageCount: 2147483647
            vary:
              producers: [1, 2, 4, 8, 16, 32]
            profiles:
              full:  { warmupSeconds: 120, measureSeconds: 300, cooldownSeconds: 60, repetitions: 3 }
              quick: { warmupSeconds: 5, measureSeconds: 15, cooldownSeconds: 5, repetitions: 1 }
            """;

    @Test
    void expandsExp1ToSixPointsAndExp2ToTwelve() throws IOException {
        assertThat(ExperimentDefinition.parse(EXP1).expand("quick", Set.of())).hasSize(6);
        assertThat(ExperimentDefinition.parse(EXP2).expand("full", Set.of())).hasSize(12);
    }

    @Test
    void appliesProfileValuesToEveryPointScenario() throws IOException {
        List<ExperimentPoint> points = ExperimentDefinition.parse(EXP2).expand("full", Set.of());

        assertThat(points).allSatisfy(p -> {
            Scenario s = p.scenario();
            assertThat(s.warmupSeconds()).isEqualTo(120);
            assertThat(s.measureSeconds()).isEqualTo(300);
            assertThat(s.cooldownSeconds()).isEqualTo(60);
            assertThat(s.repetitions()).isEqualTo(3);
            assertThat(s.messageSizeBytes()).isEqualTo(1024);
        });
        assertThat(points.stream().filter(p -> p.broker() == Broker.RABBITMQ).map(p -> p.scenario().producers()))
                .containsExactly(1, 2, 4, 8, 16, 32);
        ExperimentPoint first = points.get(0);
        assertThat(first.broker()).isEqualTo(Broker.KAFKA);
        assertThat(first.dirName()).isEqualTo("kafka__producers=1");
        assertThat(first.scenario().name()).isEqualTo("exp2-producer-scaling-kafka-producers-1");
    }

    @Test
    void derivesWarmupMessagesFromPercentWithMinimumAndLiteral() throws IOException {
        ExperimentDefinition def = ExperimentDefinition.parse(EXP1);

        List<ExperimentPoint> full = def.expand("full", Set.of(Broker.KAFKA));
        assertThat(full.stream().map(p -> p.scenario().warmupMessages())).containsExactly(1000, 10_000, 100_000);

        List<ExperimentPoint> quick = def.expand("quick", Set.of(Broker.KAFKA));
        assertThat(quick).allSatisfy(p -> assertThat(p.scenario().warmupMessages()).isEqualTo(1000));
    }

    @Test
    void filtersByBroker() throws IOException {
        List<ExperimentPoint> kafkaOnly = ExperimentDefinition.parse(EXP2).expand("quick", Set.of(Broker.KAFKA));

        assertThat(kafkaOnly).hasSize(6).allSatisfy(p -> assertThat(p.broker()).isEqualTo(Broker.KAFKA));
    }

    @Test
    void rejectsInvalidDefinitions() {
        String twoVary = EXP2.replace("producers: [1, 2, 4, 8, 16, 32]", "producers: [1, 2]\n  consumers: [1, 2]");
        assertThatThrownBy(() -> ExperimentDefinition.parse(twoVary)).hasRootCauseInstanceOf(IllegalArgumentException.class);

        String emptyValues = EXP2.replace("producers: [1, 2, 4, 8, 16, 32]", "producers: []");
        assertThatThrownBy(() -> ExperimentDefinition.parse(emptyValues)).hasRootCauseInstanceOf(IllegalArgumentException.class);

        String unknownTop = EXP2 + "bogus: 1\n";
        assertThatThrownBy(() -> ExperimentDefinition.parse(unknownTop)).isInstanceOf(IOException.class);

        String unknownScenarioField = EXP2.replace("consumers: 1", "consumers: 1\n  notAField: 3");
        assertThatThrownBy(() -> ExperimentDefinition.parse(unknownScenarioField).expand("quick", Set.of()))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> ExperimentDefinition.parse(EXP2).expand("nope", Set.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("nope");

        String rateSteps = EXP2.replace("consumers: 1", "consumers: 1\n  rateSteps: [100]");
        assertThatThrownBy(() -> ExperimentDefinition.parse(rateSteps)).hasRootCauseInstanceOf(IllegalArgumentException.class);
    }

    private static final String DURABILITY = """
            experiment: exp-durability
            brokers: [kafka, rabbitmq]
            base:
              producers: 1
              kafka: { partitions: 1, acks: "1" }
              rabbitmq: { queueType: classic, prefetch: 250 }
            vary:
              variant:
                - { label: acks-all, broker: kafka, set: { kafka: { acks: "all" } } }
                - { label: acks-0, broker: kafka, set: { kafka: { acks: "0" } } }
                - { label: quorum-confirm, broker: rabbitmq, set: { rabbitmq: { queueType: quorum, publisherConfirms: true, confirmBatchSize: 100 } } }
            profiles:
              quick: { warmupSeconds: 5, measureSeconds: 15, cooldownSeconds: 5, repetitions: 1 }
            """;

    @Test
    void variantsApplyOnlyToTheirBrokerAndMergeOverBase() throws IOException {
        List<ExperimentPoint> points = ExperimentDefinition.parse(DURABILITY).expand("quick", Set.of());

        assertThat(points).extracting(ExperimentPoint::dirName)
                .containsExactly("kafka__variant=acks-all", "kafka__variant=acks-0", "rabbitmq__variant=quorum-confirm");
        ExperimentPoint kafkaAll = points.get(0);
        assertThat(kafkaAll.scenario().kafka().acks()).isEqualTo("all");
        assertThat(kafkaAll.scenario().kafka().partitions()).isEqualTo(1); // base 값 유지
        ExperimentPoint quorum = points.get(2);
        assertThat(quorum.scenario().rabbitmq().queueType()).isEqualTo("quorum");
        assertThat(quorum.scenario().rabbitmq().publisherConfirms()).isTrue();
        assertThat(quorum.scenario().rabbitmq().confirmBatchSize()).isEqualTo(100);
        assertThat(points.get(0).scenario().rabbitmq().confirmBatchSize()).isEqualTo(1); // 기본값: 건당 동기 confirm
        assertThat(quorum.scenario().rabbitmq().prefetch()).isEqualTo(250); // base 값 유지
    }

    @Test
    void rejectsInvalidVariants() {
        String noLabel = DURABILITY.replace("label: acks-all", "name: acks-all");
        String duplicate = DURABILITY.replace("label: acks-0", "label: acks-all");
        String forbidden = DURABILITY.replace("set: { kafka: { acks: \"all\" } }", "set: { broker: rabbitmq }");

        assertThatThrownBy(() -> ExperimentDefinition.parse(noLabel)).hasRootCauseInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ExperimentDefinition.parse(duplicate)).hasRootCauseInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ExperimentDefinition.parse(forbidden)).hasRootCauseInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void profileOverridesVariantValues() throws IOException {
        String yaml = """
                experiment: exp-size
                brokers: [kafka]
                vary:
                  variant:
                    - { label: big, set: { messageSizeBytes: 1000000, messageCount: 1000 } }
                profiles:
                  full:  { warmupMessagesPercent: 10, warmupMessagesMin: 100 }
                  quick: { messageCount: 500, warmupMessages: 50 }
                """;
        ExperimentDefinition def = ExperimentDefinition.parse(yaml);

        Scenario full = def.expand("full", Set.of()).get(0).scenario();
        assertThat(full.messageCount()).isEqualTo(1000);
        assertThat(full.warmupMessages()).isEqualTo(100);
        Scenario quick = def.expand("quick", Set.of()).get(0).scenario();
        assertThat(quick.messageCount()).isEqualTo(500); // 프로파일이 variant 를 덮어쓴다
        assertThat(quick.messageSizeBytes()).isEqualTo(1000000);
    }
}
