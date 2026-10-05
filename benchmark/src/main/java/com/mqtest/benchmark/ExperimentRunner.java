package com.mqtest.benchmark;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.mqtest.common.AggregateWriter;
import com.mqtest.common.Aggregator;
import com.mqtest.common.Broker;
import com.mqtest.common.ExperimentDefinition;
import com.mqtest.common.ExperimentPlanner;
import com.mqtest.common.ExperimentPoint;
import com.mqtest.common.ExperimentSummary;
import com.mqtest.common.PointStore;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 실험 정의를 (broker × 변수값) 포인트로 확장해 순차 실행한다.
 * 결과 구조: {@code <resultsDir>/<experiment>/<profile>/<broker>__<변수>=<값>/} + {@code experiment-meta.json},
 * {@code experiment-summary.csv}. DONE 이 있는 포인트는 건너뛴다(--force 로 재실행).
 */
final class ExperimentRunner {

    private static final Logger log = LoggerFactory.getLogger(ExperimentRunner.class);
    private static final ObjectMapper JSON = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    private final Path definitionFile;
    private final String profile;
    private final Set<Broker> brokers;
    private final Path resultsDir;
    private final boolean force;
    private final ResourceCollector collector;

    ExperimentRunner(Path definitionFile, String profile, Set<Broker> brokers, Path resultsDir, boolean force,
                     ResourceCollector collector) {
        this.definitionFile = definitionFile;
        this.profile = profile;
        this.brokers = brokers;
        this.resultsDir = resultsDir;
        this.force = force;
        this.collector = collector;
    }

    /** 브로커에 연결하지 않고 실행 계획과 시간 상한 추정만 출력한다. */
    void dryRun() throws IOException {
        ExperimentDefinition def = ExperimentDefinition.load(definitionFile);
        List<ExperimentPoint> points = def.expand(profile, brokers);
        Path root = root(def);
        List<ExperimentPoint> pending = ExperimentPlanner.pending(root, points, force);
        System.out.printf("experiment=%s profile=%s points=%d (pending=%d, skipped as DONE=%d)%n",
                def.experiment(), profile, points.size(), pending.size(), points.size() - pending.size());
        for (ExperimentPoint p : points) {
            boolean skip = !pending.contains(p);
            System.out.printf("  %-34s runs=%d warmup=%ds measure=%ds cooldown=%ds%s%n", p.dirName(),
                    p.scenario().repetitions(), p.scenario().warmupSeconds(), p.scenario().measureSeconds(),
                    p.scenario().cooldownSeconds(), skip ? "  [DONE, skip]" : "");
        }
        long seconds = ExperimentPlanner.estimateSeconds(pending);
        System.out.printf("total runs=%d, estimated duration (upper bound): %dh %02dm%n",
                ExperimentPlanner.runCount(pending), seconds / 3600, (seconds % 3600) / 60);
    }

    void run() throws Exception {
        ExperimentDefinition def = ExperimentDefinition.load(definitionFile);
        List<ExperimentPoint> points = def.expand(profile, brokers);
        Path root = root(def);
        Files.createDirectories(root);
        Map<String, String> environment = Environment.collect();
        writeMeta(def, root, points, environment);

        List<ExperimentPoint> pending = ExperimentPlanner.pending(root, points, force);
        log.info("experiment {} [{}]: {} points, {} pending", def.experiment(), profile, points.size(), pending.size());
        int n = 0;
        for (ExperimentPoint p : pending) {
            log.info("=== point {}/{}: {} ===", ++n, pending.size(), p.dirName());
            Path dir = PointStore.dir(root, p);
            PointStore.clear(dir);
            List<Aggregator.StepAggregate> aggregates =
                    new SuiteRunner(p.scenario(), environment, dir, collector).run();
            PointStore.writeAggregate(dir, aggregates.get(0));
            PointStore.markDone(dir);
            writeSummary(def, root); // 포인트마다 갱신해 중간에 멈춰도 표가 남는다
        }
        Path csv = writeSummary(def, root);
        log.info("summary: {}", csv);
    }

    private Path root(ExperimentDefinition def) {
        return resultsDir.resolve(def.experiment()).resolve(profile);
    }

    /** --brokers 로 일부만 실행했더라도 요약은 정의 전체 포인트 중 DONE 인 것을 모두 포함한다. */
    private Path writeSummary(ExperimentDefinition def, Path root) throws IOException {
        List<ExperimentSummary.Row> rows = new ArrayList<>();
        for (ExperimentPoint p : def.expand(profile, Set.of())) {
            Path dir = PointStore.dir(root, p);
            if (PointStore.isDone(dir)) {
                rows.add(new ExperimentSummary.Row(p, PointStore.readAggregate(dir)));
            }
        }
        return ExperimentSummary.write(root, def.experiment(), profile, rows);
    }

    private void writeMeta(ExperimentDefinition def, Path root, List<ExperimentPoint> points,
                           Map<String, String> environment) throws IOException {
        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("experiment", def.experiment());
        meta.put("profile", profile);
        meta.put("definitionSha256", sha256(definitionFile));
        meta.put("gitRevision", command("git", "rev-parse", "HEAD"));
        meta.put("startedAt", Instant.now().toString());
        meta.put("brokers", points.stream().map(p -> p.broker().name()).distinct().toList());
        meta.put("pointCount", points.size());
        meta.put("runningContainers", List.of(command("docker", "ps", "--format", "{{.Names}}").split("\\R")).stream()
                .filter(c -> c.startsWith("mqt-")).sorted().toList());
        meta.put("environment", environment);
        JSON.writeValue(root.resolve("experiment-meta.json").toFile(), meta);
    }

    private static String sha256(Path file) throws IOException {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file)));
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String command(String... cmd) {
        try {
            Process proc = new ProcessBuilder(cmd).redirectErrorStream(true).start();
            String out = new String(proc.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
            return proc.waitFor() == 0 ? out : "unknown";
        } catch (Exception e) {
            return "unknown";
        }
    }
}
