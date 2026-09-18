package com.assignment.beam.validation;

import com.assignment.beam.model.CustomerField;
import com.assignment.beam.model.CustomerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * CustomerRecordValidator class Validates each records and writes the error
 * records in the error file along with the violation.
 */
public final class CustomerRecordValidator {

    private CustomerRecordValidator() {
        throw new AssertionError("utility class — do not instantiate");
    }

    public static List<String> validate(CustomerRecord record) {
        List<String> violations = new ArrayList<>();
        for (CustomerField field : CustomerField.values()) {
            checkField(violations, field, record.get(field));
        }
        return violations;
    }

    private static void checkField(List<String> violations, CustomerField field, String rawValue) {
        String value = rawValue == null ? "" : rawValue.trim();
        if (value.isEmpty()) {
            if (field.required()) {
                violations.add(field.dbColumn() + " is required but missing");
            }
            return;
        }
        if (value.length() < field.minLength()) {
            violations.add(field.dbColumn() + " shorter than min length " + field.minLength());
        } else if (value.length() > field.maxLength()) {
            violations.add(field.dbColumn() + " exceeds max length " + field.maxLength() + " chars");
        }
        if (field.pattern() != null) {
            if (field.list()) {
                String[] items = value.split("[,;|]");
                if (items.length == 0 || (items.length == 1 && items[0].trim().isEmpty())) {
                    violations.add(field.dbColumn() + " has no usable items");
                }
                for (String item : items) {
                    String trimmed = item.trim();
                    if (!trimmed.isEmpty() && !Pattern.matches(field.pattern(), trimmed)) {
                        violations.add(field.dbColumn() + " item '" + trimmed + "' fails pattern /"
                                + field.pattern() + "/");
                        break;
                    }
                }
            } else if (!Pattern.matches(field.pattern(), value)) {
                violations.add(field.dbColumn() + " '" + value + "' fails pattern /"
                        + field.pattern() + "/");
            }
        }
    }
}
