package com.assignment.ingest.dto;

/**
 * IngestionResponse returns the response to the API Request.
 */
public class IngestionResponse {

    private final String executionId;
    private final String dagRunId;
    private final String status;
    private final String message;
    private final boolean splitApplied;

    public IngestionResponse(String executionId, String dagRunId, String status,
                             String message, boolean splitApplied) {
        this.executionId = executionId;
        this.dagRunId = dagRunId;
        this.status = status;
        this.message = message;
        this.splitApplied = splitApplied;
    }

    public String getExecutionId() { return executionId; }
    public String getDagRunId() { return dagRunId; }
    public String getStatus() { return status; }
    public String getMessage() { return message; }
    public boolean isSplitApplied() { return splitApplied; }
}
