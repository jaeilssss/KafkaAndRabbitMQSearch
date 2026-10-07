package com.mqtest.benchmark;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 실험 포인트가 실행되는 동안 호스트(macOS)의 전원/열 상태를 주기적으로 기록한다(pmset).
 * 열 경고가 기록되면 그 구간의 처리량은 브로커가 아니라 노트북의 열 한계를 반영할 수 있다.
 * pmset 이 없는 환경(Linux 등)에서는 아무것도 하지 않는다. Apple Silicon 은 스로틀링 정도를 pmset 으로
 * 완전히 노출하지 않으므로, 경고가 없다는 것이 스로틀링이 없었다는 증명은 아니다.
 */
final class HostStateSampler implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(HostStateSampler.class);
    private static final Pattern PERCENT = Pattern.compile("(\\d+)%");
    private static final String HEADER = "time,onAC,batteryPercent,thermalWarning,performanceWarning,cpuSpeedLimit\n";

    private final Path file;
    private final Duration interval;
    private final Thread thread;
    private volatile boolean warned;

    private HostStateSampler(Path file, Duration interval) {
        this.file = file;
        this.interval = interval;
        this.thread = new Thread(this::loop, "host-state-sampler");
        this.thread.setDaemon(true);
    }

    /** pmset 을 쓸 수 없으면 null 을 반환한다. */
    static HostStateSampler start(Path file, Duration interval) {
        if (output("pmset", "-g", "therm") == null) {
            return null;
        }
        try {
            Files.createDirectories(file.getParent());
            Files.writeString(file, HEADER);
        } catch (IOException e) {
            log.warn("host state log disabled: {}", e.toString());
            return null;
        }
        HostStateSampler s = new HostStateSampler(file, interval);
        s.thread.start();
        return s;
    }

    private void loop() {
        while (!Thread.currentThread().isInterrupted()) {
            sample();
            try {
                Thread.sleep(interval.toMillis());
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    private void sample() {
        String therm = output("pmset", "-g", "therm");
        String batt = output("pmset", "-g", "batt");
        if (therm == null || batt == null) {
            return;
        }
        boolean thermal = therm.contains("Thermal_Warning_Level") || (therm.contains("thermal warning") && !therm.contains("No thermal warning"));
        boolean performance = therm.contains("Performance_Warning_Level") || (therm.contains("performance warning") && !therm.contains("No performance warning"));
        String limit = value(therm, "CPU_Speed_Limit");
        Matcher m = PERCENT.matcher(batt);
        String percent = m.find() ? m.group(1) : "";
        if (thermal || performance || (!limit.isEmpty() && !limit.equals("100"))) {
            if (!warned) {
                log.warn("host thermal/performance warning detected - results of this point may reflect laptop throttling ({})", file);
            }
            warned = true;
        }
        try {
            Files.writeString(file, "%s,%s,%s,%s,%s,%s%n".formatted(Instant.now(), batt.contains("AC Power"), percent, thermal,
                    performance, limit), java.nio.file.StandardOpenOption.APPEND);
        } catch (IOException e) {
            log.warn("host state write failed: {}", e.toString());
        }
    }

    private static String value(String text, String key) {
        Matcher m = Pattern.compile(key + "\\s*=\\s*(\\d+)").matcher(text);
        return m.find() ? m.group(1) : "";
    }

    @Override
    public void close() {
        thread.interrupt();
        try {
            thread.join(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        sample(); // 종료 시점의 상태를 한 번 더 기록
    }

    private static String output(String... cmd) {
        try {
            Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
            String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            return p.waitFor() == 0 ? out : null;
        } catch (Exception e) {
            return null;
        }
    }
}
