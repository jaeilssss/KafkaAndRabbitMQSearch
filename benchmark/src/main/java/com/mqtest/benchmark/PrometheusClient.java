package com.mqtest.benchmark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mqtest.common.Broker;
import com.mqtest.common.ResourceMetrics;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Prometheus(cAdvisor 지표)에서 브로커 컨테이너의 CPU / 메모리 / 네트워크 / 디스크를 측정 구간 기준으로 조회한다.
 * 윈도우 평균은 {@code rate(counter[W])} 를 구간 끝 시각에 평가해 얻는다.
 */
final class PrometheusClient implements ResourceCollector {

    private static final Logger log = LoggerFactory.getLogger(PrometheusClient.class);
    private static final int MIN_WINDOW_SECONDS = 10; // scrape 간격(5s) x 2

    private final String baseUrl;
    private final Duration settle;
    private final java.util.function.Function<String, String> containerIds;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
    private final ObjectMapper json = new ObjectMapper();

    /** @param settle 구간 종료 후 마지막 scrape 가 반영되길 기다리는 시간 */
    PrometheusClient(String baseUrl, Duration settle) {
        this(baseUrl, settle, PrometheusClient::dockerContainerId);
    }

    /**
     * @param containerIds 컨테이너 이름 -> 전체 컨테이너 ID. Docker Desktop(macOS) 의 cAdvisor 는 {@code name} 라벨이 없고
     *                     {@code id="/docker/<id>"} 로만 구분되므로, ID 를 알 수 있으면 id 로 선택하고 아니면 name 으로 선택한다.
     */
    PrometheusClient(String baseUrl, Duration settle, java.util.function.Function<String, String> containerIds) {
        this.containerIds = containerIds;
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.settle = settle;
    }

    @Override
    public ResourceMetrics collect(Broker broker, Instant start, Instant end) {
        String name = broker == Broker.KAFKA ? "mqt-kafka" : "mqt-rabbitmq";
        long window = Math.max(MIN_WINDOW_SECONDS, (long) Math.ceil(Duration.between(start, end).toMillis() / 1000.0));
        String w = window + "s";
        String id = containerIds.apply(name);
        String sel = id == null ? "{name=\"" + name + "\"}" : "{id=\"/docker/" + id + "\"}";
        try {
            Thread.sleep(settle.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return ResourceMetrics.empty();
        }
        long time = end.getEpochSecond();
        return new ResourceMetrics(
                query("sum(rate(container_cpu_usage_seconds_total" + sel + "[" + w + "]))", time),
                query("max_over_time(sum(rate(container_cpu_usage_seconds_total" + sel + "[15s]))[" + w + ":5s])", time),
                query("max_over_time(sum(container_memory_working_set_bytes" + sel + ")[" + w + ":5s])", time),
                network("receive", sel, w, time),
                network("transmit", sel, w, time),
                query("sum(rate(container_fs_writes_bytes_total" + sel + "[" + w + "]))", time));
    }

    private static final int STEP_SECONDS = 5; // scrape 간격과 같다

    /**
     * 구간의 시계열을 {@code time,<series>...} 형태의 CSV 로 저장한다. 값이 없는 시각은 빈 칸이다
     * (예: Kafka consumer group 이 없는 구간의 lag). Prometheus 를 쓸 수 없으면 파일을 만들지 않는다.
     */
    @Override
    public void recordTimeSeries(Broker broker, Instant start, Instant end, Path file) {
        String name = broker == Broker.KAFKA ? "mqt-kafka" : "mqt-rabbitmq";
        String id = containerIds.apply(name);
        String sel = id == null ? "{name=\"" + name + "\"}" : "{id=\"/docker/" + id + "\"}";
        Map<String, String> series = new LinkedHashMap<>();
        series.put("cpuCores", "sum(rate(container_cpu_usage_seconds_total" + sel + "[15s]))");
        series.put("memoryBytes", "sum(container_memory_working_set_bytes" + sel + ")");
        series.put("diskWriteBytesPerSec", "sum(rate(container_fs_writes_bytes_total" + sel + "[15s]))");
        series.put("netRxBytesPerSec", "sum(rate(container_network_receive_bytes_total{id=\"/\",interface=\"eth0\"}[15s]))");
        series.put("netTxBytesPerSec", "sum(rate(container_network_transmit_bytes_total{id=\"/\",interface=\"eth0\"}[15s]))");
        series.put(broker == Broker.KAFKA ? "consumerLag" : "queueDepth",
                broker == Broker.KAFKA ? "sum(kafka_consumergroup_lag)" : "sum(rabbitmq_queue_messages)");
        try {
            Thread.sleep(settle.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
        Map<Long, Map<String, String>> rows = new TreeMap<>();
        boolean any = false;
        for (Map.Entry<String, String> e : series.entrySet()) {
            Map<Long, String> points = queryRange(e.getValue(), start.getEpochSecond(), end.getEpochSecond());
            any |= !points.isEmpty();
            points.forEach((t, v) -> rows.computeIfAbsent(t, k -> new LinkedHashMap<>()).put(e.getKey(), v));
        }
        if (!any) {
            log.warn("no time series available, skipped: {}", file);
            return;
        }
        StringBuilder sb = new StringBuilder("time,elapsedSec");
        series.keySet().forEach(k -> sb.append(',').append(k));
        sb.append('\n');
        for (Map.Entry<Long, Map<String, String>> row : rows.entrySet()) {
            sb.append(Instant.ofEpochSecond(row.getKey())).append(',').append(row.getKey() - start.getEpochSecond());
            for (String k : series.keySet()) {
                sb.append(',').append(row.getValue().getOrDefault(k, ""));
            }
            sb.append('\n');
        }
        try {
            Files.createDirectories(file.getParent());
            Files.writeString(file, sb);
        } catch (IOException ex) {
            log.warn("failed to write time series {}: {}", file, ex.toString());
        }
    }

    private Map<Long, String> queryRange(String promql, long startSec, long endSec) {
        Map<Long, String> out = new TreeMap<>();
        try {
            String url = baseUrl + "/api/v1/query_range?query=" + URLEncoder.encode(promql, StandardCharsets.UTF_8)
                    + "&start=" + startSec + "&end=" + endSec + "&step=" + STEP_SECONDS;
            HttpResponse<String> res = http.send(
                    HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(10)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            if (res.statusCode() != 200) {
                log.warn("prometheus range query failed ({}): {}", res.statusCode(), promql);
                return out;
            }
            JsonNode result = json.readTree(res.body()).path("data").path("result");
            if (result.isArray() && !result.isEmpty()) {
                for (JsonNode v : result.get(0).path("values")) {
                    String value = v.path(1).asText("");
                    if (!value.equals("NaN") && !value.isEmpty()) {
                        out.put(v.path(0).asLong(), String.format(Locale.ROOT, "%.4f", Double.parseDouble(value)));
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.warn("prometheus unavailable, time series skipped: {}", e.toString());
        }
        return out;
    }

    /**
     * 컨테이너 단위 네트워크 지표를 우선 쓰고, 없으면(Docker Desktop 의 cAdvisor 는 컨테이너별 네트워크를 노출하지 않음)
     * VM 전체의 eth0 지표로 대체한다. 이 경우 같은 VM 의 다른 컨테이너 트래픽이 포함될 수 있다.
     */
    private Double network(String direction, String selector, String window, long time) {
        String metric = "container_network_" + direction + "_bytes_total";
        Double perContainer = query("sum(rate(" + metric + selector + "[" + window + "]))", time);
        if (perContainer != null) {
            return perContainer;
        }
        return query("sum(rate(" + metric + "{id=\"/\",interface=\"eth0\"}[" + window + "]))", time);
    }

    private static String dockerContainerId(String name) {
        try {
            Process p = new ProcessBuilder("docker", "inspect", "-f", "{{.Id}}", name).redirectErrorStream(true).start();
            String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
            return p.waitFor() == 0 && out.matches("[0-9a-f]{12,64}") ? out : null;
        } catch (Exception e) {
            return null;
        }
    }

    private Double query(String promql, long time) {
        try {
            String url = baseUrl + "/api/v1/query?query=" + URLEncoder.encode(promql, StandardCharsets.UTF_8) + "&time=" + time;
            HttpResponse<String> res = http.send(
                    HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(5)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            if (res.statusCode() != 200) {
                log.warn("prometheus query failed ({}): {}", res.statusCode(), promql);
                return null;
            }
            JsonNode result = json.readTree(res.body()).path("data").path("result");
            if (!result.isArray() || result.isEmpty()) {
                return null;
            }
            String value = result.get(0).path("value").path(1).asText(null);
            if (value == null || value.equals("NaN")) {
                return null;
            }
            return Double.valueOf(value);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (Exception e) {
            log.warn("prometheus unavailable, resource metric skipped: {}", e.toString());
            return null;
        }
    }
}
