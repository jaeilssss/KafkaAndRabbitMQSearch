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
 * <p>변수 이름이 {@code variant} 이면 값은 이름 붙은 설정 묶음이다:
 * {@code {label: acks-all, broker: kafka, set: {kafka: {acks: "all"}}}}. {@code label} 은 결과 디렉터리 이름이 되고,
 * {@code broker} 가 있으면 그 브로커의 포인트에만 쓰이며, {@code set} 은 base 위에 깊은 병합으로 덮어쓴다.
 * 브로커마다 바꿀 설정이 다른 실험(내구성 비교 등)에 쓴다.
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
    static final String VARIANT = "variant";
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
        if (vary.containsKey(VARIANT)) {
            validateVariants(vary.get(VARIANT), brokers);
        }
    }

    private static void validateVariants(List<JsonNode> variants, List<Broker> brokers) {
        Set<String> labels = new java.util.HashSet<>();
        for (JsonNode v : variants) {
            if (!v.isObject() || !v.hasNonNull("label") || !NAME.matcher(v.get("label").asText()).matches()) {
                throw new IllegalArgumentException("each variant needs a 'label' ([A-Za-z0-9_.-]+)");
            }
            if (!labels.add(v.get("label").asText())) {
                throw new IllegalArgumentException("duplicate variant label: " + v.get("label").asText());
            }
            if (v.has("set") && !v.get("set").isObject()) {
                throw new IllegalArgumentException("variant '" + v.get("label").asText() + "': set must be a mapping");
            }
            if (v.has("set")) {
                for (String forbidden : FORBIDDEN_IN_BASE) {
                    if (v.get("set").has(forbidden)) {
                        throw new IllegalArgumentException("'" + forbidden + "' cannot be set in a variant");
                    }
                }
            }
            if (v.hasNonNull("broker") && !brokers.contains(Broker.valueOf(v.get("broker").asText().toUpperCase()))) {
                throw new IllegalArgumentException("variant '" + v.get("label").asText() + "' targets a broker not in this experiment");
            }
        }
    }

    private static void merge(ObjectNode target, JsonNode overrides) {
        overrides.fields().forEachRemaining(e -> {
            if (e.getValue().isObject() && target.get(e.getKey()) instanceof ObjectNode child) {
                merge(child, e.getValue());
            } else {
                target.set(e.getKey(), e.getValue());
            }
        });
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
                boolean variant = variable.getKey().equals(VARIANT);
                if (variant && value.hasNonNull("broker")
                        && Broker.valueOf(value.get("broker").asText().toUpperCase()) != broker) {
                    continue;
                }
                String valueText = variant ? value.get("label").asText() : value.asText();
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
                if (variant) {
                    if (value.has("set")) {
                        merge(tree, value.get("set"));
                    }
                } else {
                    tree.set(variable.getKey(), value);
                }
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
