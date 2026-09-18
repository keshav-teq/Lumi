package com.assignment.beam.parsers;

import com.assignment.beam.model.CustomerRecord;
import com.assignment.beam.model.CustomerField;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;

/**
 * JsonRecordParser class parses the JSON files.
 */
public class JsonRecordParser implements Serializable {

    private transient ObjectMapper mapper;

    ObjectMapper mapper() {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        return mapper;
    }

  /**
   * Parses each JSON record as a single line.
   */
  public CustomerRecord parse(String line) {
        if (line == null || line.trim().isEmpty()) {
            throw new IllegalArgumentException("empty line");
        }
        JsonNode root;
        try {
            root = mapper().readTree(line);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("invalid JSON: " + e.getOriginalMessage(), e);
        }
        if (!root.isObject()) {
            throw new IllegalArgumentException(
                    "expected a JSON object per line, got " + root.getNodeType() + ": " + line);
        }
        CustomerRecord record = new CustomerRecord();
        for (CustomerField field : CustomerField.values()) {
            record.set(field, extract(root, field));
        }
        return record.cleanse();
    }

  /**
   * Extracts each JSON Record.
   */
    private static String extract(JsonNode root, CustomerField field) {
        JsonNode node = root.get(field.jsonTopKey());
        if (node != null && !field.jsonPath().equals(field.jsonTopKey())) {
            node = node.get(field.jsonLeafKey());
        }
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isArray()) {
            StringBuilder joined = new StringBuilder();
            for (JsonNode item : node) {
                if (joined.length() > 0) {
                    joined.append(", ");
                }
                joined.append(item.asText());
            }
            return joined.toString();
        }
        return node.asText();
    }
}

