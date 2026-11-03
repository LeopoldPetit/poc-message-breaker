package com.github.leopoldpetit.pocmb.common;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

/** Unbuffered append log: each line reaches the kernel before the call returns, so it survives SIGKILL. */
public final class AuditLog {

    private final OutputStream out;

    private AuditLog(OutputStream out) {
        this.out = out;
    }

    /** Returns null when POC_AUDIT_DIR is unset. Files go to POC_AUDIT_DIR/role-containerId.csv. */
    public static AuditLog fromEnv(String role) {
        String base = System.getenv("POC_AUDIT_DIR");
        if (base == null || base.isBlank()) {
            return null;
        }
        String host = System.getenv().getOrDefault("HOSTNAME", "local");
        try {
            Path dir = Path.of(base);
            java.nio.file.Files.createDirectories(dir);
            return new AuditLog(new FileOutputStream(dir.resolve(role + "-" + host + ".csv").toFile(), true));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public synchronized void record(String line) {
        try {
            out.write((line + "\n").getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
