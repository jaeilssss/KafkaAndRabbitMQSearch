package com.mqtest.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

/** 실험 포인트 디렉터리의 저장/재개 규칙. */
public final class PointStore {

    static final String DONE = "DONE";
    static final String AGGREGATE = "point-result.json";

    private static final ObjectMapper JSON = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    private PointStore() {
    }

    public static Path dir(Path experimentRoot, ExperimentPoint point) {
        return experimentRoot.resolve(point.dirName());
    }

    public static boolean isDone(Path dir) {
        return Files.exists(dir.resolve(DONE));
    }

    public static void writeAggregate(Path dir, Aggregator.StepAggregate aggregate) throws IOException {
        Files.createDirectories(dir);
        JSON.writeValue(dir.resolve(AGGREGATE).toFile(), aggregate);
    }

    public static Aggregator.StepAggregate readAggregate(Path dir) throws IOException {
        return JSON.readValue(dir.resolve(AGGREGATE).toFile(), Aggregator.StepAggregate.class);
    }

    public static void markDone(Path dir) throws IOException {
        Files.writeString(dir.resolve(DONE), java.time.Instant.now().toString() + "\n");
    }

    public static void clear(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(dir)) {
            for (Path p : walk.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(p);
            }
        }
    }
}
