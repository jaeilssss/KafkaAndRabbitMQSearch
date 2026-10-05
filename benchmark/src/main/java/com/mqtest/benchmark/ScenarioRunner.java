package com.mqtest.benchmark;

import com.mqtest.common.Broker;
import com.mqtest.common.GeneratorHealth;
import com.mqtest.common.PacedLoop;
import com.mqtest.common.RatePacer;
import com.mqtest.common.ResourceMetrics;
import com.mqtest.common.LatencyStats;
import com.mqtest.common.MessageConsumerGroup;
import com.mqtest.common.MessagePublisher;
import com.mqtest.common.Payload;
import com.mqtest.common.RunResult;
import com.mqtest.common.Scenario;
import com.mqtest.consumer.KafkaMessageConsumerGroup;
import com.mqtest.consumer.RabbitMessageConsumerGroup;
import com.mqtest.producer.KafkaMessagePublisher;
import com.mqtest.producer.RabbitMessagePublisher;
import java.lang.management.ManagementFactory;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Scenario 1건을 warm-up → measurement → cool-down 순서로 실행하고 결과를 집계한다. */
final class ScenarioRunner {

    private static final Logger log = LoggerFactory.getLogger(ScenarioRunner.class);

    private final Scenario s;
    private final Map<String, String> environment;

    private final LatencyStats latency = new LatencyStats();
    private final AtomicLong sent = new AtomicLong();
    private final AtomicLong received = new AtomicLong();
    private final AtomicLong measuredSent = new AtomicLong();
    private final AtomicLong measuredReceived = new AtomicLong();
    private final LatencyStats scheduleLag = new LatencyStats();
    private volatile long lastMeasuredReceiveNanos;
    private volatile long measureStartNanos = Long.MAX_VALUE;
    private final AtomicLong firstMeasuredSendNanos = new AtomicLong(Long.MAX_VALUE);
    private final ResourceCollector collector;
    private final Instant wallRef = Instant.now();
    private final long nanoRef = System.nanoTime();

    ScenarioRunner(Scenario scenario, Map<String, String> environment) {
        this(scenario, environment, null);
    }

    /** @param collector null 이면 자원 지표를 수집하지 않는다 */
    ScenarioRunner(Scenario scenario, Map<String, String> environment, ResourceCollector collector) {
        this.s = scenario;
        this.environment = environment;
        this.collector = collector;
    }

    private Instant wallClock(long nanos) {
        return wallRef.plusNanos(nanos - nanoRef);
    }

    RunResult run() throws Exception {
        String resource = "mq-" + s.name().replaceAll("[^A-Za-z0-9_.-]", "-") + "-" + UUID.randomUUID().toString().substring(0, 8);
        String startedAt = Instant.now().toString();
        log.info("run '{}' on {} -> {}", s.name(), s.broker(), resource);

        createTopology(resource);
        List<MessagePublisher> publishers = new ArrayList<>();
        try (MessageConsumerGroup consumers = newConsumers(resource)) {
            consumers.start(this::onMessage);
            Thread.sleep(2_000); // consumer 할당/구독 안정화

            long startNanos = System.nanoTime();
            long measureStart = startNanos + TimeUnit.SECONDS.toNanos(s.warmupSeconds());
            measureStartNanos = measureStart;
            long deadlineNanos = measureStart + TimeUnit.SECONDS.toNanos(s.measureSeconds());
            AtomicLong sequence = new AtomicLong();
            Double rate = s.targetRatePerSec();
            List<RatePacer> pacers = rate == null ? null : RatePacer.split(rate, s.producers(), startNanos);
            List<Thread> threads = new ArrayList<>();
            for (int i = 0; i < s.producers(); i++) {
                MessagePublisher publisher = newPublisher(resource);
                publishers.add(publisher);
                RatePacer pacer = pacers == null ? null : pacers.get(i);
                Runnable body = pacer == null
                        ? () -> produce(publisher, sequence, measureStart, deadlineNanos)
                        : () -> producePaced(publisher, pacer, sequence, measureStart, deadlineNanos);
                Thread t = new Thread(body, "producer-" + i);
                threads.add(t);
                t.start();
            }
            CpuWindow cpu = new CpuWindow();
            PacedLoop.sleepUntilPublic(measureStart);
            cpu.begin();
            for (Thread t : threads) {
                t.join();
            }
            cpu.end();
            publishers.forEach(MessagePublisher::flush);
            long producersDoneNanos = System.nanoTime();

            awaitDrain(producersDoneNanos);

            long windowStart = windowStart(measureStart);
            long windowEnd = Math.min(producersDoneNanos, deadlineNanos);
            ResourceMetrics resources = collector == null ? null
                    : collector.collect(s.broker(), wallClock(windowStart), wallClock(windowEnd));
            return buildResult(startedAt, measureStart, deadlineNanos, producersDoneNanos, cpu.load(), resources);
        } finally {
            publishers.forEach(MessagePublisher::close);
            deleteTopology(resource);
        }
    }

    private void produce(MessagePublisher publisher, AtomicLong sequence, long measureStart, long deadlineNanos) {
        long seq;
        while ((seq = sequence.getAndIncrement()) < s.messageCount()) {
            long now = System.nanoTime();
            if (now >= deadlineNanos) {
                break;
            }
            publisher.publish(Payload.encode(seq, now, s.messageSizeBytes()));
            sent.incrementAndGet();
            if (seq >= s.warmupMessages() && now >= measureStart) {
                measuredSent.incrementAndGet();
                if (s.warmupMessages() > 0) {
                    firstMeasuredSendNanos.accumulateAndGet(now, Math::min);
                }
            }
        }
    }

    /** 개수 기반 warm-up 이면 첫 측정 메시지를 발행한 시각, 아니면 measureStart. */
    private long windowStart(long measureStart) {
        long first = firstMeasuredSendNanos.get();
        return s.warmupMessages() > 0 && first != Long.MAX_VALUE ? first : measureStart;
    }

    private void producePaced(MessagePublisher publisher, RatePacer pacer, AtomicLong sequence,
                              long measureStart, long deadlineNanos) {
        PacedLoop.run(publisher, pacer, s.messageSizeBytes(), sequence, s.messageCount(), deadlineNanos,
                (scheduled, actual) -> {
                    sent.incrementAndGet();
                    if (scheduled >= measureStart) {
                        measuredSent.incrementAndGet();
                        scheduleLag.recordNanos(Math.max(actual - scheduled, 1));
                    }
                });
    }

    private void onMessage(byte[] payload) {
        long now = System.nanoTime();
        if (s.consumerDelayMs() > 0) {
            try {
                Thread.sleep(s.consumerDelayMs());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        received.incrementAndGet();
        long sentNanos = Payload.sendNanos(payload);
        if (sentNanos >= measureStartNanos && Payload.sequence(payload) >= s.warmupMessages()) { // warm-up 구간에 보낸 메시지는 집계에서 제외
            latency.recordNanos(System.nanoTime() - sentNanos);
            measuredReceived.incrementAndGet();
            lastMeasuredReceiveNanos = now;
        }
    }

    /**
     * 발행 종료 후 consumer 가 남은 메시지를 모두 소비할 때까지 기다린다. 소비가 {@code cooldownSeconds} 동안
     * 전혀 진행되지 않으면 중단한다(과부하로 쌓인 backlog 가 소비되는 중이면 계속 기다린다). 절대 상한은 30분.
     * 중단 시점에 남은 메시지가 {@code lost} 로 보고된다.
     */
    private void awaitDrain(long producersDoneNanos) throws InterruptedException {
        long stallLimit = TimeUnit.SECONDS.toNanos(s.cooldownSeconds());
        long hardLimit = producersDoneNanos + TimeUnit.MINUTES.toNanos(30);
        long lastReceived = received.get();
        long lastProgress = producersDoneNanos;
        while (received.get() < sent.get()) {
            long now = System.nanoTime();
            long current = received.get();
            if (current != lastReceived) {
                lastReceived = current;
                lastProgress = now;
            }
            if (now - lastProgress > stallLimit || now > hardLimit) {
                break;
            }
            Thread.sleep(50);
        }
    }

    private RunResult buildResult(String startedAt, long measureStart, long deadlineNanos, long producersDoneNanos,
                                  double cpuLoad, ResourceMetrics resources) {
        long windowStart = windowStart(measureStart);
        // 발행 윈도우는 measure 구간 끝(deadline)을 넘지 않는다(flush 시간 제외).
        double producerWindow = Math.max(nanosToSeconds(Math.min(producersDoneNanos, deadlineNanos) - windowStart), 1e-9);
        double consumerWindow = Math.max(nanosToSeconds(lastMeasuredReceiveNanos - windowStart), 1e-9);
        double producerRate = measuredSent.get() / producerWindow;
        double consumerRate = lastMeasuredReceiveNanos == 0 ? 0 : measuredReceived.get() / consumerWindow;
        double bytes = s.messageSizeBytes();
        double lagP99 = scheduleLag.snapshot().p99Ms();
        boolean saturated = GeneratorHealth.isSaturated(lagP99, cpuLoad);
        if (saturated) {
            log.warn("load generator saturated (scheduleLagP99={}ms, processCpu={}%): 처리량/지연이 브로커 한계가 아닐 수 있음",
                    String.format("%.1f", lagP99), String.format("%.0f", cpuLoad * 100));
        }
        return new RunResult(
                s.name(), s.broker(), startedAt, s.messageSizeBytes(), s.producers(), s.consumers(),
                sent.get(), received.get(), sent.get() - received.get(),
                measuredSent.get(), measuredReceived.get(), producerWindow,
                producerRate, consumerRate,
                producerRate * bytes / 1_000_000.0, consumerRate * bytes / 1_000_000.0,
                latency.snapshot(), s, environment,
                s.targetRatePerSec(), lagP99, cpuLoad, saturated, resources);
    }

    private static double nanosToSeconds(long nanos) {
        return nanos / 1_000_000_000.0;
    }

    private void createTopology(String resource) throws Exception {
        if (s.broker() == Broker.KAFKA) {
            BrokerTopology.createKafkaTopic(s.kafka(), resource);
        } else {
            BrokerTopology.declareRabbitQueue(s.rabbitmq(), resource);
        }
    }

    private void deleteTopology(String resource) {
        if (s.broker() == Broker.KAFKA) {
            BrokerTopology.deleteKafkaTopic(s.kafka(), resource);
        } else {
            BrokerTopology.deleteRabbitQueue(s.rabbitmq(), resource);
        }
    }

    private MessagePublisher newPublisher(String resource) {
        return s.broker() == Broker.KAFKA
                ? new KafkaMessagePublisher(s.kafka(), resource)
                : new RabbitMessagePublisher(s.rabbitmq(), resource);
    }

    private MessageConsumerGroup newConsumers(String resource) {
        return s.broker() == Broker.KAFKA
                ? new KafkaMessageConsumerGroup(s.kafka(), resource, resource + "-group", s.consumers())
                : new RabbitMessageConsumerGroup(s.rabbitmq(), resource, s.consumers());
    }

    /** 측정 구간 동안 러너 JVM 프로세스의 평균 CPU 사용률(0~1, 전체 코어 대비). */
    private static final class CpuWindow {
        private final com.sun.management.OperatingSystemMXBean os =
                (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        private long cpu0;
        private long wall0;
        private long cpu1;
        private long wall1;

        void begin() {
            cpu0 = os.getProcessCpuTime();
            wall0 = System.nanoTime();
        }

        void end() {
            cpu1 = os.getProcessCpuTime();
            wall1 = System.nanoTime();
        }

        double load() {
            long wall = wall1 - wall0;
            if (wall <= 0 || cpu0 < 0 || cpu1 < 0) {
                return 0;
            }
            return (double) (cpu1 - cpu0) / (wall * (double) os.getAvailableProcessors());
        }
    }
}
