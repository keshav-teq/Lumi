package com.assignment.beam.validation;

import java.util.Collections;
import java.util.List;

/**
 * RecordValidationException Class.
 */
public class RecordValidationException extends RuntimeException {

    private final List<String> violations;

    public RecordValidationException(String recordId, List<String> violations) {
        super("record '" + recordId + "': " + String.join("; ", violations));
        this.violations = Collections.unmodifiableList(violations);
    }

    public List<String> getViolations() {
        return violations;
    }
}