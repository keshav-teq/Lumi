package com.assignment.beam.control;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.OptionalLong;
import java.util.Properties;

/**
 * ControlFileReader is a helper class created to handle the Control Files passed along with records.
 * It reads the recordCount from the Control File, verifies the records passed to the database
 * compares them and verifies if both the record counts match or not.
 */
public final class ControlFileReader {

    public static final String RECORD_COUNT_KEY = "record_count";

    private ControlFileReader() {
        throw new AssertionError("utility class — do not instantiate");
    }

    public static OptionalLong readRecordCount(String controlFilePath) {
        if (controlFilePath == null || controlFilePath.trim().isEmpty()) {
            return OptionalLong.empty();
        }
        Path path = Path.of(controlFilePath.trim());
        if (!Files.isRegularFile(path)) {
            throw new IllegalArgumentException("control file not found: " + path);
        }
        Properties props = new Properties();
        try (Reader r = new InputStreamReader(Files.newInputStream(path), StandardCharsets.UTF_8)) {
            props.load(r);
        } catch (IOException e) {
            throw new IllegalStateException("cannot read control file " + path, e);
        }
        String raw = props.getProperty(RECORD_COUNT_KEY);
        if (raw == null || raw.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "control file " + path + " is missing the '" + RECORD_COUNT_KEY + "' attribute");
        }
        try {
            return OptionalLong.of(Long.parseLong(raw.trim()));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "control file " + RECORD_COUNT_KEY + " is not an integer: '" + raw + "'", e);
        }
    }

    public static void assertMatches(long expected, long loaded, long rejected, String executionId) {
        if (loaded != expected) {
            throw new IllegalStateException(String.format(
                    "RECORD COUNT CHECK FAILED (execution %s): expected %d loaded records "
                            + "(per control file), but actual ingested = %d, dead-lettered to error "
                            + "file = %d (delta %+d)",
                    executionId, expected, loaded, rejected, loaded - expected));
        }
    }
}
