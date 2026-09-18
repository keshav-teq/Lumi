package com.assignment.ingest.service;

import com.assignment.ingest.config.AirflowConfig;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * SparkSplitService initializes the Spark Splitting Function to split the files
 * breaching the threshold value.
 */
@Service
public class SparkSplitService {

    private static final Logger LOG = LoggerFactory.getLogger(SparkSplitService.class);

    private final AirflowConfig props;

    public SparkSplitService(AirflowConfig props) {
        this.props = props;
    }

    /** Compares the file size, in Bytes, with the threshold value.*/
    public boolean exceedsThreshold(long sizeBytes) {
        long threshold = props.getIngestion().getSplitThresholdBytes();
        return threshold > 0 && sizeBytes > threshold;
    }

    /** Triggers the PySpark to split the input file to splitted chunks.*/
    public void splitIntoChunks(Path hostInputFile, Path hostOutputDir) {
        AirflowConfig.Spark spark = props.getSpark();
        List<String> cmd = List.of(
                spark.getPython(), spark.getScript(),
                "--input", hostInputFile.toString(),
                "--output-dir", hostOutputDir.toString(),
                "--chunk-lines", String.valueOf(spark.getChunkLines()));

        LOG.info("Splitting {} via PySpark -> {}", hostInputFile, hostOutputDir);
        try {
            Process process = new ProcessBuilder(cmd).inheritIO().start();
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new IllegalStateException(
                        "PySpark split failed with exit code " + exitCode + " (cmd: "
                                + String.join(" ", cmd) + ")");
            }
        } catch (IOException e) {
            throw new IllegalStateException("could not launch PySpark split: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("PySpark split interrupted", e);
        }
    }
}
