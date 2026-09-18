package com.assignment.beam;

import com.assignment.beam.control.ControlFileReader;
import com.assignment.beam.model.CustomerField;
import com.assignment.beam.model.CustomerRecord;
import com.assignment.beam.transforms.ParseAndEnrichFn;
import com.assignment.beam.config.DatabaseConfig;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main IngestionPipeline class which initializes the whole Beam Ingestion Pipeline,
 * Reads everything from the source, writes everything to the Database, and
 * the errors to the error files.
 */
public class IngestionPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(IngestionPipeline.class);

    public static void main(String[] args) {
        IngestionOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(IngestionOptions.class);

        OptionalLong expectedCount =
                ControlFileReader.readRecordCount(options.getControlFile());

        LOG.info("Starting ingestion: fileType={} file={} executionId={} controlFile={}",
                options.getFileType(), options.getFilePath(), options.getExecutionId(),
                options.getControlFile().isEmpty() ? "<none>" : options.getControlFile());

        Pipeline p = Pipeline.create(options);

        List<PCollection<String>> reads = new ArrayList<>();
        String[] sources = options.getFilePath().split(",");
        for (int i = 0; i < sources.length; i++) {
            String src = sources[i].trim();
            if (src.isEmpty()) {
                continue;
            }
            reads.add(p.apply("ReadSourceFile" + (sources.length > 1 ? "#" + i : ""),
                    TextIO.read().from(src)));
        }
        if (reads.isEmpty()) {
            throw new IllegalArgumentException("no readable source in --filePath: "
                    + options.getFilePath());
        }
        PCollection<String> lines = reads.size() == 1
                ? reads.get(0)
                : PCollectionList.of(reads).apply("MergeSplitChunks", Flatten.pCollections());

        PCollectionTuple parsed = lines.apply("ParseAndEnrich",
                ParDo.of(new ParseAndEnrichFn(options.getFileType(), options.getExecutionId()))
                        .withOutputTags(ParseAndEnrichFn.GOOD_TAG,
                                TupleTagList.of(ParseAndEnrichFn.ERROR_TAG)));

        PCollection<CustomerRecord> goodRecords = parsed.get(ParseAndEnrichFn.GOOD_TAG);
        PCollection<String> errorLines = parsed.get(ParseAndEnrichFn.ERROR_TAG);

        String columns = Stream.concat(
                        Stream.of(CustomerField.values()).map(CustomerField::dbColumn),
                        Stream.of("ingestion_timestamp", "execution_id", "source_creation_time"))
                .collect(Collectors.joining(", "));
        String placeholders = Stream.of(CustomerField.values())
                .map(f -> "?")
                .reduce((a, b) -> a + "," + b)
                .orElse("") + ",?,?,?";
        String insertSql = String.format("INSERT INTO CUSTOMERS (%s) VALUES (%s)",
                columns, placeholders);

        goodRecords.apply("WriteToWarehouse",
                JdbcIO.<CustomerRecord>write()
                        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                                .create("com.mysql.cj.jdbc.Driver", DatabaseConfig.JDBC_URL)
                                .withUsername(DatabaseConfig.DB_USERNAME)
                                .withPassword(DatabaseConfig.DB_PASSWORD))
                        .withStatement(insertSql)
                        .withPreparedStatementSetter((CustomerRecord r, PreparedStatement ps) -> {
                            int i = 1;
                            for (CustomerField field : CustomerField.values()) {
                                ps.setString(i++, r.get(field));
                            }
                            ps.setTimestamp(i++, Timestamp.valueOf(r.getIngestionTimestamp()));
                            ps.setString(i++, r.getExecutionId());
                            ps.setTimestamp(i, Timestamp.valueOf(r.getSourceCreationTime()));
                        }));

        errorLines.apply("WriteErrors",
                TextIO.write()
                        .to(options.getErrorOutput())
                        .withSuffix(".txt")
                        .withNumShards(1));

        PipelineResult result = p.run();
        result.waitUntilFinish();

        long loaded = counter(result, ParseAndEnrichFn.GOOD_ROWS_METRIC);
        long rejected = counter(result, ParseAndEnrichFn.ERROR_ROWS_METRIC);
        LOG.info("Run summary: {} loaded, {} dead-lettered (executionId={})",
                loaded, rejected, options.getExecutionId());

        if (expectedCount.isPresent()) {
            ControlFileReader.assertMatches(expectedCount.getAsLong(), loaded, rejected,
                    options.getExecutionId());
            LOG.info("Record-count check PASSED: {} rows as per control file {}",
                    loaded, options.getControlFile());
        }
        LOG.info("Ingestion finished for executionId={}", options.getExecutionId());
    }

    private static long counter(PipelineResult result, String metricName) {
        long total = 0;
        for (MetricResult<Long> m : result.metrics()
                .queryMetrics(MetricsFilter.builder()
                        .addNameFilter(MetricNameFilter.named(ParseAndEnrichFn.METRIC_NAMESPACE, metricName))
                        .build())
                .getCounters()) {
            Long v = m.getCommittedOrNull() != null ? m.getCommittedOrNull() : m.getAttempted();
            if (v != null) {
                total += v;
            }
        }
        return total;
    }
}
