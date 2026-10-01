package com.mqtest.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Locale;

public final class ResultWriter {

    static final String CSV_HEADER = "startedAt,scenario,broker,messageSizeBytes,producers,consumers,sent,received,lost,"
            + "producerMsgPerSec,consumerMsgPerSec,producerMBPerSec,consumerMBPerSec,"
            + "p50Ms,p95Ms,p99Ms,p999Ms,maxMs";

    private static final ObjectMapper JSON = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    private ResultWriter() {
    }

    /** {@code <dir>/<scenario>-<ts>.json} 저장 후 {@code <dir>/summary.csv} 에 한 줄 추가. JSON 경로를 반환한다. */
    public static Path write(Path dir, RunResult r) throws IOException {
        Files.createDirectories(dir);
        String stamp = r.startedAt().replace(":", "").replace(".", "");
        Path json = dir.resolve(r.scenario() + "-" + stamp + ".json");
        JSON.writeValue(json.toFile(), r);

        Path csv = dir.resolve("summary.csv");
        StringBuilder sb = new StringBuilder();
        if (!Files.exists(csv)) {
            sb.append(CSV_HEADER).append('\n');
        }
        sb.append(csvRow(r)).append('\n');
        Files.writeString(csv, sb, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        return json;
    }

    static String csvRow(RunResult r) {
        LatencyStats.Percentiles l = r.latency();
        return String.format(Locale.ROOT,
                "%s,%s,%s,%d,%d,%d,%d,%d,%d,%.2f,%.2f,%.4f,%.4f,%.3f,%.3f,%.3f,%.3f,%.3f",
                r.startedAt(), r.scenario(), r.broker(), r.messageSizeBytes(), r.producers(), r.consumers(),
                r.sent(), r.received(), r.lost(),
                r.producerMsgPerSec(), r.consumerMsgPerSec(), r.producerMBPerSec(), r.consumerMBPerSec(),
                l.p50Ms(), l.p95Ms(), l.p99Ms(), l.p999Ms(), l.maxMs());
    }
}
