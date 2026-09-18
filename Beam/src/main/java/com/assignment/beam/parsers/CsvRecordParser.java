package com.assignment.beam.parsers;

import com.assignment.beam.model.CustomerRecord;
import com.assignment.beam.model.CustomerField;

import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.io.Serializable;

/**
 * CsvRecordParser class parses the CSV Files.
 */
public class CsvRecordParser implements Serializable {
    /**
     * Checks if the first line is HEADER in a data file.
     */
    public static final String HEADER_PREFIX = "customer_id";

    public boolean isHeader(String line) {
        return line != null && line.trim().startsWith(HEADER_PREFIX);
    }

    /**
     * Parses the CSV file line by line; splitting each column by commas.
     */
    public CustomerRecord parse(String line) {
        if (line == null || line.trim().isEmpty()) {
            throw new IllegalArgumentException("empty line");
        }
        CustomerField[] schema = CustomerField.values();
        String[] parts = line.split(",", -1);
        if (parts.length != schema.length) {
            throw new IllegalArgumentException(
                    "expected " + schema.length + " CSV columns but got " + parts.length + ": " + line);
        }
        CustomerRecord record = new CustomerRecord();
        for (int i = 0; i < schema.length; i++) {
            record.set(schema[i], parts[i].trim());
        }
        return record.cleanse();
    }

    public static String header() {
        return Stream.of(schema()).map(CustomerField::jsonLeafKey).collect(Collectors.joining(","));
    }

    private static CustomerField[] schema() {
        return CustomerField.values();
    }
}

