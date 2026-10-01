package com.mqtest.benchmark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/** 결과 JSON 에 실험 환경(계획서 §7)을 함께 기록한다. */
final class Environment {

    private Environment() {
    }

    static Map<String, String> collect() {
        Map<String, String> env = new LinkedHashMap<>();
        env.put("os", System.getProperty("os.name") + " " + System.getProperty("os.version") + " " + System.getProperty("os.arch"));
        env.put("jdk", System.getProperty("java.vendor") + " " + System.getProperty("java.version"));
        env.put("cpus", String.valueOf(Runtime.getRuntime().availableProcessors()));
        env.put("jvmMaxHeapMB", String.valueOf(Runtime.getRuntime().maxMemory() / (1024 * 1024)));
        env.put("springBoot", org.springframework.boot.SpringBootVersion.getVersion());
        // docker/.env 에 고정한 브로커/모니터링 이미지 태그
        Path dotEnv = Path.of("docker", ".env");
        if (Files.exists(dotEnv)) {
            try {
                for (String line : Files.readAllLines(dotEnv)) {
                    line = line.strip();
                    if (!line.isEmpty() && !line.startsWith("#") && line.contains("=")) {
                        String[] kv = line.split("=", 2);
                        env.put(kv[0], kv[1]);
                    }
                }
            } catch (IOException ignored) {
                // 환경 정보 수집 실패가 실험을 막아서는 안 된다.
            }
        }
        return env;
    }
}
