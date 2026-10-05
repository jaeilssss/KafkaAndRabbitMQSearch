package com.mqtest.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * 실험 정의(YAML). {@code base} 에 공통 Scenario 필드, {@code vary} 에 바꿀 변수 1개와 값 목록,
 * {@code profiles} 에 실행 시간 조건(full / quick …)을 둔다.
 *
 * <p>profile 에서만 쓸 수 있는 보조 필드: {@code warmupMessagesPercent}, {@code warmupMessagesMin}
 * (메시지 수 대비 비율로 warmupMessages 를 유도).
 */
public record ExperimentDefinition(
        String experiment,
        String description,
        List<Broker> brokers,
        JsonNode base,
        Map<String, List<JsonNode>> vary,
        Map<String, JsonNode> profiles) {

    private static final Pattern NAME = Pattern.compile("[A-Za-z0-9_.-]+");
    private static final Set<String> FORBIDDEN_IN_BASE = Set.of("name", "broker", "rateSteps");
    private static final ObjectMapper MAPPER = ScenarioLoader.yamlMapper().copy();

    public ExperimentDefinition {
        if (experiment == null || !NAME.matcher(experiment).matches()) {
            throw new IllegalArgumentException("experiment name is required ([A-Za-z0-9_.-]+)");
        }
        if (brokers == null || brokers.isEmpty()) {
            throw new IllegalArgumentException("brokers must not be empty");
        }
        if (vary == null || vary.size() != 1) {
            throw new IllegalArgumentException("vary must define exactly one variable");
        }
        if (vary.values().iterator().next() == null || vary.values().iterator().next().isEmpty()) {
            throw new IllegalArgumentException("vary values must not be empty");
        }
        if (profiles == null || profiles.isEmpty()) {
            throw new IllegalArgumentException("profiles must not be empty");
        }
        base = base == null ? MAPPER.createObjectNode() : base;
        if (!base.isObject()) {
            throw new IllegalArgumentException("base must be a mapping");
        }
        for (String forbidden : FORBIDDEN_IN_BASE) {
            if (base.has(forbidden) || vary.containsKey(forbidden)) {
                throw new IllegalArgumentException("'" + forbidden + "' cannot be set in base/vary");
            }
        }
        brokers = List.copyOf(brokers);
    }

    public static ExperimentDefinition parse(String yaml) throws IOException {
        return MAPPER.readValue(yaml, ExperimentDefinition.class);
    }

    public static ExperimentDefinition load(Path file) throws IOException {
        return parse(Files.readString(file));
    }

    /**
     * @param brokerFilter 비어 있으면 정의의 모든 broker
     */
    public List<ExperimentPoint> expand(String profile, Set<Broker> brokerFilter) {
        JsonNode profileNode = profiles.get(profile);
        if (profileNode == null || !profileNode.isObject()) {
            throw new IllegalArgumentException("unknown profile '" + profile + "' (available: " + profiles.keySet() + ")");
        }
        for (Broker b : brokerFilter) {
            if (!brokers.contains(b)) {
                throw new IllegalArgumentException("broker " + b + " is not part of experiment " + experiment);
            }
        }
        Map.Entry<String, List<JsonNode>> variable = vary.entrySet().iterator().next();

        List<ExperimentPoint> points = new ArrayList<>();
        for (Broker broker : brokers) {
            if (!brokerFilter.isEmpty() && !brokerFilter.contains(broker)) {
                continue;
            }
            for (JsonNode value : variable.getValue()) {
                String valueText = value.asText();
                ObjectNode tree = ((ObjectNode) base).deepCopy();
                Integer percent = null;
                Integer min = null;
                for (Iterator<Map.Entry<String, JsonNode>> it = profileNode.fields(); it.hasNext(); ) {
                    var e = it.next();
                    switch (e.getKey()) {
                        case "warmupMessagesPercent" -> percent = e.getValue().asInt();
                        case "warmupMessagesMin" -> min = e.getValue().asInt();
                        default -> tree.set(e.getKey(), e.getValue());
                    }
                }
                tree.set(variable.getKey(), value);
                tree.put("broker", broker.name().toLowerCase());
                tree.put("name", experiment + "-" + broker.name().toLowerCase() + "-" + variable.getKey() + "-" + valueText);
                if (percent != null || min != null) {
                    JsonNode count = tree.get("messageCount");
                    if (count == null) {
                        throw new IllegalArgumentException("warmupMessagesPercent requires messageCount");
                    }
                    long derived = Math.max(min == null ? 0 : min, percent == null ? 0 : Math.round(count.asLong() * percent / 100.0));
                    tree.put("warmupMessages", derived);
                }
                points.add(new ExperimentPoint(experiment, profile, broker, variable.getKey(), valueText,
                        ScenarioLoader.fromTree(tree)));
            }
        }
        return points;
    }
}
