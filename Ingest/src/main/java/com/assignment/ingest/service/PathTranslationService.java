package com.assignment.ingest.service;

import com.assignment.ingest.config.AirflowConfig;
import java.io.File;
import java.nio.file.Path;
import java.util.Optional;
import org.springframework.stereotype.Service;

/**
 * Translates between the Host Machine and the Container.
 *
 * The paths are translated between the Host Machine and Container Machine.
 */
@Service
public class PathTranslationService {

    private final AirflowConfig props;

    public PathTranslationService(AirflowConfig props) {
        this.props = props;
    }

    /**
     * The Container Path is translated to the Host Path.
     */
    public Optional<Path> containerDataToHost(String containerPath) {
        AirflowConfig.Paths p = props.getPaths();
        if (containerPath == null || p.getHostDataDir().isBlank()
                || !containerPath.startsWith(p.getContainerDataPrefix())) {
            return Optional.empty();
        }
        String rest = containerPath.substring(p.getContainerDataPrefix().length());
        while (!rest.isEmpty() && (rest.charAt(0) == '/' || rest.charAt(0) == '\\')) {
            rest = rest.substring(1);
        }
        rest = rest.replace('/', File.separatorChar);
        return Optional.of(Path.of(p.getHostDataDir()).resolve(rest).normalize());
    }

    /** The OutputDir used by Spark to write the splitted chunks. */
    public Path splitOutputDirFor(String executionId) {
        return Path.of(props.getPaths().getHostSplitDir()).resolve(executionId);
    }

    /** The Directory passed by the DAG and read by the Beam to process the data. */
    public String splitGlobFor(String executionId) {
        String prefix = props.getPaths().getContainerSplitPrefix();
        if (prefix.endsWith("/")) {
            prefix = prefix.substring(0, prefix.length() - 1);
        }
        return prefix + "/" + executionId + "/part-*";
    }
}