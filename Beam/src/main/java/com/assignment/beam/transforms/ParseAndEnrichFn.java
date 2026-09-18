package com.assignment.beam.transforms;

import com.assignment.beam.model.CustomerRecord;
import com.assignment.beam.model.CustomerField;
import com.assignment.beam.parsers.CsvRecordParser;
import com.assignment.beam.parsers.JsonRecordParser;
import com.assignment.beam.security.Encoder;
import com.assignment.beam.validation.CustomerRecordValidator;
import com.assignment.beam.validation.RecordValidationException;

import java.time.LocalDateTime;
import java.util.List;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;

/**
 * ParseAndEnrichFn DoFn class creates CustomerRecord in the database while writing error records in the error file.
 * This class is responsible for calling the respective Parser based on file type.
 */
public class ParseAndEnrichFn extends DoFn<String, CustomerRecord> {

    public static final TupleTag<CustomerRecord> GOOD_TAG = new TupleTag<>("good") {};
    public static final TupleTag<String> ERROR_TAG = new TupleTag<>("errors") {};

    public static final String METRIC_NAMESPACE = "lumi";
    public static final String GOOD_ROWS_METRIC = "good_rows";
    public static final String ERROR_ROWS_METRIC = "error_rows";

    private static final Counter GOOD_ROWS = Metrics.counter(METRIC_NAMESPACE, GOOD_ROWS_METRIC);
    private static final Counter ERROR_ROWS = Metrics.counter(METRIC_NAMESPACE, ERROR_ROWS_METRIC);

    private final String fileType;
    private final String executionId;

    private final CsvRecordParser csv = new CsvRecordParser();
    private final JsonRecordParser json = new JsonRecordParser();

    public ParseAndEnrichFn(String fileType, String executionId) {
        this.fileType = fileType.toUpperCase();
        this.executionId = executionId;
    }

    /**
     * Processes Each Element Line by Line.
     */
    @ProcessElement
    public void processElement(@Element String line, MultiOutputReceiver out) {
        if ("CSV".equals(fileType) && csv.isHeader(line)) {
            return;
        }
        if (line == null || line.trim().isEmpty()) {
            return;
        }
        try {
            CustomerRecord rec;

            switch (fileType) {
                case "CSV":
                    rec = csv.parse(line);
                    break;
                case "JSON":
                    rec = json.parse(line);
                    break;
                default:
                    throw new IllegalArgumentException("unknown fileType: " + fileType);
            }

            List<String> violations = CustomerRecordValidator.validate(rec);

            if (!violations.isEmpty()) {
                throw new RecordValidationException(rec.get(CustomerField.CUSTOMER_ID), violations);
            }

            rec.setSourceCreationTime(LocalDateTime.now());
            rec.setIngestionTimestamp(LocalDateTime.now());
            rec.setExecutionId(executionId);

            for (CustomerField field : CustomerField.values()) {
                String value = rec.get(field);
                if (field.sensitive() && !" ".equals(value)) {
                    rec.set(field, Encoder.encode(value));
                }
            }

            GOOD_ROWS.inc();
            out.get(GOOD_TAG).output(rec);

        } catch (RecordValidationException e) {
            ERROR_ROWS.inc();
            emitError(out, line, "VALIDATION: " + e.getMessage());
        } catch (Exception e) {
            ERROR_ROWS.inc();
            emitError(out, line, "PARSE: " + e.getMessage());
        }
    }

    /**
     * Writes the error in the error file.
     */
    private void emitError(MultiOutputReceiver out, String rawLine, String reason) {
        String safeLine = rawLine.replace("\n", " ").replace("\r", " ");
        out.get(ERROR_TAG).output(
                "executionId=" + executionId + " | " + reason + " | line=" + safeLine);
    }
}
