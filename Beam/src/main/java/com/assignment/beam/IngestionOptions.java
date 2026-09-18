package com.assignment.beam;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/**
 * IngestionOptions Interface which accepts the arguments for the Beam Pipeline.
 */
public interface IngestionOptions extends PipelineOptions {
    @Description("SOR feed file, glob, or comma-separated list (CSV/JSON)")
    @Validation.Required
    String getFilePath();

    void setFilePath(String value);

    @Description("File type: CSV | JSON (case-insensitive)")
    @Validation.Required
    String getFileType();

    void setFileType(String value);

    @Description("UUID for this ingestion run")
    @Validation.Required
    String getExecutionId();

    void setExecutionId(String value);

    @Description("Prefix for the error-record file(s), e.g. /tmp/lumi-errors/errors")
    @Validation.Required
    String getErrorOutput();

    void setErrorOutput(String value);

    @Description("Properties file with record_count=<expected loaded rows>; optional")
    @Default.String("")
    String getControlFile();

    void setControlFile(String value);
}
