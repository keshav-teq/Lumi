package com.assignment.ingest.config;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** AirflowConfig File
 * AirflowConfig defines Configuration Properties to use the values
 * defined in the application.properties to create a user client
 * for Airflow.
 * */
@ConfigurationProperties(prefix = "app")
public class AirflowConfig {

    private static final Logger LOG = LoggerFactory.getLogger(AirflowConfig.class);

    private final Airflow airflow = new Airflow();
    private final Defaults defaults = new Defaults();
    private final Ingestion ingestion = new Ingestion();
    private final Paths paths = new Paths();
    private final Spark spark = new Spark();

    public Airflow getAirflow() { return airflow; }
    public Defaults getDefaults() { return defaults; }
    public Ingestion getIngestion() { return ingestion; }
    public Paths getPaths() { return paths; }
    public Spark getSpark() { return spark; }

    /**
     * The below function verifies if the PySpark Splitting is correctly
     * configured or not.
     * The directories are needed to be set up in the application.properties
     * in order for the PySpark to work.
     */
    @PostConstruct
    void verifySplittingIsFullyConfigured() {
        if (ingestion.getSplitThresholdBytes() > 0
                && (paths.getHostDataDir().isBlank() || paths.getHostSplitDir().isBlank())) {
            LOG.warn("PHASE 2 SPLITTING INERT: threshold={} but app.paths.host-data-dir='{}' "
                            + "and app.paths.host-split-dir='{}' — set BOTH to absolute HOST paths in "
                            + "application.properties (hostDataDir = the folder your compose mounts to "
                            + "{}) or every oversize file will skip splitting silently.",
                    ingestion.getSplitThresholdBytes(), paths.getHostDataDir(), paths.getHostSplitDir(),
                    paths.getContainerDataPrefix());
        }
    }

    /**
    * Initializes the Airflow with the config values.
    */
    public static class Airflow {
        private String baseUrl;
        private String username;
        private String password;
        private String dagId;

        public String getBaseUrl() { return baseUrl; }
        public void setBaseUrl(String s) { this.baseUrl = s; }
        public String getUsername() { return username; }
        public void setUsername(String s) { this.username = s; }
        public String getPassword() { return password; }
        public void setPassword(String s) { this.password = s; }
        public String getDagId() { return dagId; }
        public void setDagId(String s) { this.dagId = s; }
    }

    /**
     * Defaults pass the errorOutput path to a predefined directory.
     */
    public static class Defaults {
        private String errorOutput;

        public String getErrorOutput() { return errorOutput; }
        public void setErrorOutput(String s) { this.errorOutput = s; }
    }

    /**
     * Below class initializes the Ingestion Pipelines by comparing
     * the file size with the split threshold defined in the application.properties (1MiB).
     * The paths defined are used to route the HOST data dir to the CONTAINER data dir.
     */
    public static class Ingestion {
        private long splitThresholdBytes;

        public long getSplitThresholdBytes() { return splitThresholdBytes; }
        public void setSplitThresholdBytes(long v) { this.splitThresholdBytes = v; }
    }

    public static class Paths {
        private String containerDataPrefix = "/opt/airflow/data";
        private String hostDataDir = "";
        private String containerSplitPrefix = "/opt/airflow/split";
        private String hostSplitDir = "";

        public String getContainerDataPrefix() { return containerDataPrefix; }
        public void setContainerDataPrefix(String s) { this.containerDataPrefix = s; }
        public String getHostDataDir() { return hostDataDir; }
        public void setHostDataDir(String s) { this.hostDataDir = s; }
        public String getContainerSplitPrefix() { return containerSplitPrefix; }
        public void setContainerSplitPrefix(String s) { this.containerSplitPrefix = s; }
        public String getHostSplitDir() { return hostSplitDir; }
        public void setHostSplitDir(String s) { this.hostSplitDir = s; }
    }

    /**
     * Spark initializes the Spark command with the arguments and python command.
     */
    public static class Spark {
        private String python = "python";
        private String script = "../spark/split_files.py";
        private int chunkLines = 5000;

        public String getPython() { return python; }
        public void setPython(String s) { this.python = s; }
        public String getScript() { return script; }
        public void setScript(String s) { this.script = s; }
        public int getChunkLines() { return chunkLines; }
        public void setChunkLines(int v) { this.chunkLines = v; }
    }
}
