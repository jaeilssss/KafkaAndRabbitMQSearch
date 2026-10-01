package com.mqtest.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public final class AggregateWriter {

    private static final ObjectMapper JSON = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    private static final String[] METRICS = {"producerMsgPerSec", "consumerMsgPerSec", "p50Ms", "p95Ms", "p99Ms", "lost"};

    private AggregateWriter() {
    }

    /** sweepCsv 는 단계 상승(sweep)이 아니면 null 이다. */
    public record Paths(Path aggregateJson, Path aggregateCsv, Path sweepCsv) {
    }

    public static Paths write(Path dir, String scenario, String stamp, List<Aggregator.StepAggregate> steps,
                              boolean sweep) throws IOException {
        Files.createDirectories(dir);
        Path json = dir.resolve(scenario + "-aggregate-" + stamp + ".json");
        JSON.writeValue(json.toFile(), steps);

        Path csv = dir.resolve(scenario + "-aggregate-" + stamp + ".csv");
        Files.write(csv, aggregateCsv(steps));

        Path sweepCsv = null;
        if (sweep) {
            sweepCsv = dir.resolve(scenario + "-sweep-" + stamp + ".csv");
            Files.write(sweepCsv, sweepCsv(steps));
        }
        return new Paths(json, csv, sweepCsv);
    }

    static List<String> sweepCsv(List<Aggregator.StepAggregate> steps) {
        List<String> lines = new ArrayList<>();
        lines.add("targetRatePerSec,runs,consumerMsgPerSec,achievedRatio,p99Ms,lostTotal,generatorSaturated");
        for (Aggregator.StepAggregate s : steps) {
            lines.add(String.format(Locale.ROOT, "%s,%d,%.2f,%s,%.3f,%d,%b",
                    rate(s.targetRatePerSec()), s.runs(), s.consumerMsgPerSec().mean(), ratio(s.achievedRatio()),
                    s.p99Ms().mean(), s.lostTotal(), s.generatorSaturated()));
        }
        return lines;
    }

    static List<String> aggregateCsv(List<Aggregator.StepAggregate> steps) {
        StringBuilder header = new StringBuilder("targetRatePerSec,runs");
        for (String m : METRICS) {
            header.append(',').append(m).append("_mean,").append(m).append("_median,").append(m).append("_stddev");
        }
        header.append(",lostTotal,achievedRatio,generatorSaturated");
        List<String> lines = new ArrayList<>();
        lines.add(header.toString());
        for (Aggregator.StepAggregate s : steps) {
            StringBuilder row = new StringBuilder(rate(s.targetRatePerSec())).append(',').append(s.runs());
            for (Aggregator.Stat st : new Aggregator.Stat[] {s.producerMsgPerSec(), s.consumerMsgPerSec(), s.p50Ms(),
                    s.p95Ms(), s.p99Ms(), s.lost()}) {
                row.append(String.format(Locale.ROOT, ",%.3f,%.3f,%.3f", st.mean(), st.median(), st.stddev()));
            }
            row.append(',').append(s.lostTotal()).append(',').append(ratio(s.achievedRatio())).append(',')
                    .append(s.generatorSaturated());
            lines.add(row.toString());
        }
        return lines;
    }

    private static String rate(Double r) {
        return r == null ? "unlimited" : r.toString();
    }

    private static String ratio(Double r) {
        return r == null ? "" : String.format(Locale.ROOT, "%.4f", r);
    }
}
