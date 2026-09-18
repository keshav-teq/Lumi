package com.assignment.ingest.service;

import com.assignment.ingest.config.AirflowConfig;
import com.assignment.ingest.dto.IngestionRequest;
import com.assignment.ingest.dto.IngestionResponse;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * IngestionService initializes the IngestionService for the files provided through
 * the SpringAPI.
 */
@Service
public class IngestionService {

    private static final Logger LOG = LoggerFactory.getLogger(IngestionService.class);

    private final AirflowConfig props;
    private final AirflowTriggerService airflow;
    private final PathTranslationService pathTranslation;
    private final SparkSplitService sparkSplit;

    public IngestionService(AirflowConfig props, AirflowTriggerService airflow,
                            PathTranslationService pathTranslation, SparkSplitService sparkSplit) {
        this.props = props;
        this.airflow = airflow;
        this.pathTranslation = pathTranslation;
        this.sparkSplit = sparkSplit;
    }

    /**
     * Based on the IngestionRequest body, startIngestion initiates the Ingesttion.
     * Compares the fileSize with the threshold and if true, then triggers the SparkSplitService.
     * Else, the DAG run is triggered directly without splitting.
     */
    public IngestionResponse startIngestion(IngestionRequest req) {
        // executionId identifies every row of this run
        String executionId = UUID.randomUUID().toString();
        String dagRunId = "api-" + executionId;

        // If splitting is applied to the host path.
        String effectiveFilePath = req.getFileLocation();
        boolean splitApplied = false;
        Optional<Path> hostFile = pathTranslation.containerDataToHost(req.getFileLocation());
        if (hostFile.isPresent() && Files.isRegularFile(hostFile.get())) {
            try {
                long size = Files.size(hostFile.get());
                if (sparkSplit.exceedsThreshold(size)) {
                    Path outDir = pathTranslation.splitOutputDirFor(executionId);
                    deleteQuietly(outDir);
                    sparkSplit.splitIntoChunks(hostFile.get(), outDir);
                    effectiveFilePath = pathTranslation.splitGlobFor(executionId);
                    splitApplied = true;
                    LOG.info("Split applied for {}: {} bytes > threshold, reading {}",
                            executionId, size, effectiveFilePath);
                } else {
                    LOG.info("No split: {} bytes <= threshold {} (file {})",
                            size, props.getIngestion().getSplitThresholdBytes(), hostFile.get());
                }
            } catch (IOException e) {
                LOG.warn("could not stat {} on host; skipping split: {}",
                        req.getFileLocation(), e.getMessage());
            }
        } else {
            String prefix = props.getPaths().getContainerDataPrefix();
            LOG.info("split skipped — hostDataDir='{}' isBlank={} prefixMatch={} mapped={} exists={}",
                    props.getPaths().getHostDataDir(),
                    props.getPaths().getHostDataDir().isBlank(),
                    req.getFileLocation().startsWith(prefix),
                    hostFile,
                    hostFile.isPresent() && Files.isRegularFile(hostFile.get()));
        }

        Map<String, Object> conf = new HashMap<>();
        conf.put("file_path", effectiveFilePath);
        conf.put("file_type", req.getFileType().toUpperCase());
        conf.put("execution_id", executionId);
        conf.put("error_output", props.getDefaults().getErrorOutput());
        conf.put("control_file",
                req.getControlFile() == null ? "" : req.getControlFile());

        conf.values().removeIf(java.util.Objects::isNull);

        airflow.triggerDagRun(dagRunId, conf);

        return new IngestionResponse(executionId, dagRunId, "TRIGGERED",
                "Ingestion triggered for " + effectiveFilePath
                        + (splitApplied ? " (after PySpark split into chunks)" : "")
                        + ". Track it in Airflow (run " + dagRunId
                        + ") or query MySQL by execution_id.",
                splitApplied);
    }

    /**
     * Cleans up the directories.
     */
    private static void deleteQuietly(Path dir) {
        if (!Files.exists(dir)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.delete(p);
                } catch (IOException ignored) {
                }
            });
        } catch (IOException ignored) {
        }
    }
}
