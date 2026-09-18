package com.assignment.beam.model;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.EnumMap;
import java.util.Objects;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

/**
 * CustomerRecord Class to add columns to the database.
 */
@DefaultCoder(SerializableCoder.class)
public class CustomerRecord implements Serializable {

    private final EnumMap<CustomerField, String> values = new EnumMap<>(CustomerField.class);

    private LocalDateTime sourceCreationTime;
    private LocalDateTime ingestionTimestamp;
    private String executionId;

    public CustomerRecord set(CustomerField field, String rawValue) {
        values.put(field, rawValue);
        return this;
    }

    public String get(CustomerField field) {
        return values.get(field);
    }

    public CustomerRecord cleanse() {
        for (CustomerField field : CustomerField.values()) {
            String v = values.get(field);
            values.put(field, (v == null || v.trim().isEmpty()) ? " " : v);
        }
        return this;
    }

    public LocalDateTime getSourceCreationTime() {
        return sourceCreationTime;
    }

    public void setSourceCreationTime(LocalDateTime t) {
        this.sourceCreationTime = t;
    }

    public LocalDateTime getIngestionTimestamp() {
        return ingestionTimestamp;
    }

    public void setIngestionTimestamp(LocalDateTime t) {
        this.ingestionTimestamp = t;
    }

    public String getExecutionId() {
        return executionId;
    }

    public void setExecutionId(String s) {
        this.executionId = s;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CustomerRecord)) return false;
        return values.equals(((CustomerRecord) o).values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(values);
    }

    @Override
    public String toString() {
        return "CustomerRecord{" + values.get(CustomerField.CUSTOMER_ID) + ", fields="
                + values.size() + ", executionId=" + executionId + "}";
    }
}
