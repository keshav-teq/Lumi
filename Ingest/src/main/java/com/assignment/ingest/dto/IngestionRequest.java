package com.assignment.ingest.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

/**
 * Defines the IngestionRequest body which needs to be passed through the
 * SpringAPI.
 */
public class IngestionRequest {

    @NotBlank(message = "fileLocation is required")
    private String fileLocation;

    @NotBlank(message = "fileType is required")
    @Pattern(regexp = "(?i)CSV|JSON",
            message = "fileType must be CSV, JSON")
    private String fileType;

    private String controlFile;

    public String getFileLocation() { return fileLocation; }
    public void setFileLocation(String s) { this.fileLocation = s; }
    public String getFileType() { return fileType; }
    public void setFileType(String s) { this.fileType = s; }
    public String getControlFile() { return controlFile; }
    public void setControlFile(String s) { this.controlFile = s; }
}