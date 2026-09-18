package com.assignment.beam.security;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Encoder Util class encodes the data columns which need not be written in plain text.
 * A prefix b64 is added before each record is passed for now.
 */
public final class Encoder {

    public static final String PREFIX = "b64:";

    private Encoder() {
        throw new AssertionError("utility class — do not instantiate");
    }

    public static boolean looksEncoded(String value) {
        return value != null && value.startsWith(PREFIX);
    }

    public static String encode(String plaintext) {
        return PREFIX + Base64.getEncoder()
                .encodeToString(plaintext.getBytes(StandardCharsets.UTF_8));
    }

    public static String decode(String stored) {
        if (!looksEncoded(stored)) {
            throw new IllegalArgumentException(
                    "not produced by this format (missing " + PREFIX + " prefix)");
        }
        return new String(Base64.getDecoder().decode(stored.substring(PREFIX.length())),
                StandardCharsets.UTF_8);
    }
}
