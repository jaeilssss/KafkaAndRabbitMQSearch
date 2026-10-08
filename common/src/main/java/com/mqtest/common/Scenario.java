package com.mqtest.common;

import java.util.List;

/**
 * 실험 1회(Run)의 조건. YAML 에서 로드하며 생략된 값은 기본값으로 채워진다.
 *
 * <p>시간 의미: 시작 후 {@code warmupSeconds} 동안의 샘플은 측정에서 제외하고,
 * 이후 {@code measureSeconds} 까지(또는 {@code messageCount} 발행 완료 시) 발행한다.
 * {@code warmupMessages} 는 개수 기반 warm-up 으로, sequence 가 N 미만인 메시지를 측정에서 제외한다
 * (warmupSeconds 와 동시 사용 불가, rate 제어와도 함께 쓸 수 없다).
 * 발행 종료 후 consumer 가 남은 메시지를 모두 소비할 때까지 기다리되, 소비가 {@code cooldownSeconds} 동안
 * 전혀 진행되지 않으면 중단한다(그때 남은 메시지가 lost).
 *
 * <p>rate 제어: {@code targetRatePerSec}(전체 producer 합계 msg/s) 또는 {@code rateSteps}(단계 상승, 각 step 이
 * {@code repetitions} 회 반복)를 지정할 수 있다. 둘은 동시에 쓸 수 없고, 쓰면 시간 기반으로 종료한다
 * ({@code messageCount} 를 명시하지 않으면 상한 없음).
 */
public record Scenario(
        String name,
        Broker broker,
        Integer messageSizeBytes,
        Long messageCount,
        Integer producers,
        Integer consumers,
        Integer warmupSeconds,
        Integer warmupMessages,
        Integer measureSeconds,
        Integer cooldownSeconds,
        Integer consumerDelayMs,
        Double targetRatePerSec,
        List<Double> rateSteps,
        Integer repetitions,
        Integer pauseBetweenRunsSeconds,
        KafkaOptions kafka,
        RabbitOptions rabbitmq) {

    public static final int MIN_MESSAGE_SIZE = 16;

    public Scenario {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("scenario.name is required");
        }
        if (broker == null) {
            throw new IllegalArgumentException("scenario.broker is required (kafka | rabbitmq)");
        }
        messageSizeBytes = orDefault(messageSizeBytes, 1024);
        if (targetRatePerSec != null && rateSteps != null) {
            throw new IllegalArgumentException("targetRatePerSec and rateSteps cannot be used together");
        }
        if (targetRatePerSec != null && targetRatePerSec <= 0) {
            throw new IllegalArgumentException("targetRatePerSec must be > 0");
        }
        if (rateSteps != null && (rateSteps.isEmpty() || rateSteps.stream().anyMatch(r -> r == null || r <= 0))) {
            throw new IllegalArgumentException("rateSteps must be a non-empty list of positive rates");
        }
        rateSteps = rateSteps == null ? null : List.copyOf(rateSteps);
        boolean rateControlled = targetRatePerSec != null || rateSteps != null;
        messageCount = messageCount == null ? Long.valueOf(rateControlled ? Long.MAX_VALUE : 10_000) : messageCount;
        repetitions = orDefault(repetitions, 1);
        pauseBetweenRunsSeconds = orDefault(pauseBetweenRunsSeconds, 5);
        if (repetitions < 1 || pauseBetweenRunsSeconds < 0) {
            throw new IllegalArgumentException("repetitions must be >= 1 and pauseBetweenRunsSeconds >= 0");
        }
        producers = orDefault(producers, 1);
        consumers = orDefault(consumers, 1);
        warmupSeconds = orDefault(warmupSeconds, 0);
        warmupMessages = orDefault(warmupMessages, 0);
        measureSeconds = orDefault(measureSeconds, 60);
        cooldownSeconds = orDefault(cooldownSeconds, 10);
        consumerDelayMs = orDefault(consumerDelayMs, 0);
        kafka = kafka == null ? KafkaOptions.defaults() : kafka;
        rabbitmq = rabbitmq == null ? RabbitOptions.defaults() : rabbitmq;

        if (warmupMessages < 0) {
            throw new IllegalArgumentException("warmupMessages must be >= 0");
        }
        if (warmupMessages > 0) {
            if (warmupSeconds > 0) {
                throw new IllegalArgumentException("warmupMessages and warmupSeconds cannot be used together");
            }
            if (rateControlled) {
                throw new IllegalArgumentException("warmupMessages cannot be combined with targetRatePerSec/rateSteps");
            }
            if (warmupMessages >= messageCount) {
                throw new IllegalArgumentException("warmupMessages must be < messageCount");
            }
        }
        if (messageSizeBytes < MIN_MESSAGE_SIZE) {
            throw new IllegalArgumentException("messageSizeBytes must be >= " + MIN_MESSAGE_SIZE);
        }
        if (messageCount <= 0 || producers <= 0 || consumers <= 0 || measureSeconds <= 0) {
            throw new IllegalArgumentException("messageCount, producers, consumers, measureSeconds must be > 0");
        }
        if (warmupSeconds < 0 || cooldownSeconds < 0 || consumerDelayMs < 0) {
            throw new IllegalArgumentException("warmupSeconds, cooldownSeconds, consumerDelayMs must be >= 0");
        }
    }

    /** 단계 상승 시나리오에서 한 step 의 Run 용 시나리오를 만든다(rateSteps 는 제거). */
    public Scenario withTargetRate(Double rate) {
        return new Scenario(name, broker, messageSizeBytes, messageCount, producers, consumers, warmupSeconds,
                warmupMessages, measureSeconds, cooldownSeconds, consumerDelayMs, rate, null, repetitions, pauseBetweenRunsSeconds,
                kafka, rabbitmq);
    }

    private static Integer orDefault(Integer value, int fallback) {
        return value == null ? Integer.valueOf(fallback) : value;
    }

    public record KafkaOptions(
            String bootstrapServers,
            Integer partitions,
            Integer replicationFactor,
            String acks,
            Integer lingerMs,
            Integer batchSizeBytes,
            String compression) {

        public KafkaOptions {
            bootstrapServers = bootstrapServers == null ? "localhost:9092" : bootstrapServers;
            partitions = partitions == null ? Integer.valueOf(1) : partitions;
            replicationFactor = replicationFactor == null ? Integer.valueOf(1) : replicationFactor;
            acks = acks == null ? "1" : acks;
            lingerMs = lingerMs == null ? Integer.valueOf(0) : lingerMs;
            batchSizeBytes = batchSizeBytes == null ? Integer.valueOf(16384) : batchSizeBytes;
            compression = compression == null ? "none" : compression;
            if (partitions <= 0 || replicationFactor <= 0) {
                throw new IllegalArgumentException("kafka.partitions and kafka.replicationFactor must be > 0");
            }
        }

        public static KafkaOptions defaults() {
            return new KafkaOptions(null, null, null, null, null, null, null);
        }
    }

    public record RabbitOptions(
            String host,
            Integer port,
            String username,
            String password,
            String queueType,
            Boolean publisherConfirms,
            Integer confirmBatchSize,
            Integer prefetch) {

        public RabbitOptions {
            host = host == null ? "localhost" : host;
            port = port == null ? Integer.valueOf(5672) : port;
            username = username == null ? "guest" : username;
            password = password == null ? "guest" : password;
            queueType = queueType == null ? "classic" : queueType;
            publisherConfirms = publisherConfirms == null ? Boolean.FALSE : publisherConfirms;
            confirmBatchSize = confirmBatchSize == null ? Integer.valueOf(1) : confirmBatchSize; // 1 = 메시지마다 동기 confirm
            prefetch = prefetch == null ? Integer.valueOf(250) : prefetch;
            if (!queueType.equals("classic") && !queueType.equals("quorum")) {
                throw new IllegalArgumentException("rabbitmq.queueType must be classic or quorum");
            }
            if (prefetch <= 0) {
                throw new IllegalArgumentException("rabbitmq.prefetch must be > 0");
            }
            if (confirmBatchSize <= 0) {
                throw new IllegalArgumentException("rabbitmq.confirmBatchSize must be > 0");
            }
        }

        public static RabbitOptions defaults() {
            return new RabbitOptions(null, null, null, null, null, null, null, null);
        }
    }
}
