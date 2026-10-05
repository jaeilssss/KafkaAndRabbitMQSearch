package com.mqtest.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import com.mqtest.common.Broker;
import com.mqtest.common.ResourceMetrics;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class PrometheusClientTest {

    private HttpServer server;
    private final List<String> queries = new CopyOnWriteArrayList<>();

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop(0);
        }
    }

    private String start(boolean fail) throws Exception {
        return start(fail, false);
    }

    private String start(boolean fail, boolean noPerContainerNetwork) throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/v1/query", ex -> {
            String q = URLDecoder.decode(ex.getRequestURI().getRawQuery(), StandardCharsets.UTF_8);
            queries.add(q);
            boolean network = q.contains("network_");
            String value = network && noPerContainerNetwork ? (q.contains("interface=\"eth0\"") ? "777" : null)
                    : q.contains("max_over_time(sum(rate(") ? "2.5"
                    : q.contains("memory_working_set") ? "600000000"
                    : q.contains("cpu_usage") ? "1.25"
                    : q.contains("network_receive") ? "1000"
                    : q.contains("network_transmit") ? "2000"
                    : null;
            String body = value == null
                    ? "{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":[]}}"
                    : "{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":[{\"metric\":{},\"value\":[1,\"" + value + "\"]}]}}";
            int code = fail ? 500 : 200;
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(code, bytes.length);
            ex.getResponseBody().write(bytes);
            ex.close();
        });
        server.start();
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @Test
    void buildsQueriesForBrokerContainerAndParsesValues() throws Exception {
        PrometheusClient client = new PrometheusClient(start(false), Duration.ZERO, name -> null);
        Instant end = Instant.parse("2026-10-01T00:00:30Z");

        ResourceMetrics m = client.collect(Broker.KAFKA, end.minusSeconds(20), end);

        assertThat(m.cpuCoresAvg()).isCloseTo(1.25, within(1e-9));
        assertThat(m.cpuCoresMax()).isCloseTo(2.5, within(1e-9));
        assertThat(m.memoryMaxBytes()).isCloseTo(6.0e8, within(1.0));
        assertThat(m.netRxBytesPerSec()).isCloseTo(1000, within(1e-9));
        assertThat(m.netTxBytesPerSec()).isCloseTo(2000, within(1e-9));
        assertThat(m.diskWriteBytesPerSec()).isNull(); // 지표 없음 -> null
        assertThat(queries).isNotEmpty().allSatisfy(q -> assertThat(q).contains("name=\"mqt-kafka\""));
        assertThat(queries).anySatisfy(q -> assertThat(q).contains("[20s]").contains("time=" + end.getEpochSecond()));
    }

    @Test
    void usesRabbitContainerName() throws Exception {
        PrometheusClient client = new PrometheusClient(start(false), Duration.ZERO, name -> null);
        Instant end = Instant.parse("2026-10-01T00:00:30Z");

        client.collect(Broker.RABBITMQ, end.minusSeconds(20), end);

        assertThat(queries).allSatisfy(q -> assertThat(q).contains("name=\"mqt-rabbitmq\""));
    }

    @Test
    void selectsByContainerIdWhenNameLabelIsUnavailable() throws Exception {
        PrometheusClient client = new PrometheusClient(start(false), Duration.ZERO, name -> "abc123def456");
        Instant end = Instant.parse("2026-10-01T00:00:30Z");

        ResourceMetrics m = client.collect(Broker.KAFKA, end.minusSeconds(20), end);

        assertThat(queries).isNotEmpty().allSatisfy(q -> assertThat(q).contains("id=\"/docker/abc123def456\""));
        assertThat(m.cpuCoresAvg()).isNotNull();
    }

    @Test
    void fallsBackToVmLevelNetworkWhenPerContainerNetworkIsMissing() throws Exception {
        PrometheusClient client = new PrometheusClient(start(false, true), Duration.ZERO, name -> "abc123def456");
        Instant end = Instant.parse("2026-10-01T00:00:30Z");

        ResourceMetrics m = client.collect(Broker.KAFKA, end.minusSeconds(20), end);

        assertThat(m.netRxBytesPerSec()).isCloseTo(777, within(1e-9));
        assertThat(m.netTxBytesPerSec()).isCloseTo(777, within(1e-9));
        assertThat(m.cpuCoresAvg()).isNotNull();
    }

    @Test
    void returnsAllNullWhenPrometheusFailsOrIsUnreachable() throws Exception {
        PrometheusClient failing = new PrometheusClient(start(true), Duration.ZERO);
        Instant end = Instant.now();
        assertThat(failing.collect(Broker.KAFKA, end.minusSeconds(20), end)).isEqualTo(ResourceMetrics.empty());

        PrometheusClient unreachable = new PrometheusClient("http://127.0.0.1:1", Duration.ZERO);
        assertThat(unreachable.collect(Broker.KAFKA, end.minusSeconds(20), end)).isEqualTo(ResourceMetrics.empty());
    }
}
