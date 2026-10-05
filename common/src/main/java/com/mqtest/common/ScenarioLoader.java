package com.mqtest.common;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;

public final class ScenarioLoader {

    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory())
            .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS)
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private ScenarioLoader() {
    }

    static ObjectMapper yamlMapper() {
        return YAML;
    }

    /** 실험 정의에서 합성한 JSON 트리를 Scenario 로 변환한다(알 수 없는 필드는 거부). */
    public static Scenario fromTree(com.fasterxml.jackson.databind.JsonNode tree) {
        try {
            return YAML.treeToValue(tree, Scenario.class);
        } catch (IOException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw new IllegalArgumentException("invalid scenario: " + cause.getMessage(), e);
        }
    }

    public static Scenario load(Path file) throws IOException {
        try (Reader reader = Files.newBufferedReader(file)) {
            return YAML.readValue(reader, Scenario.class);
        }
    }

    public static Scenario parse(String yaml) throws IOException {
        return YAML.readValue(new StringReader(yaml), Scenario.class);
    }
}
