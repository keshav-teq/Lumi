package com.assignment.ingest.service;

import com.assignment.ingest.config.AirflowConfig;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

/**
 * AirflowTriggerService defines the service to trigger the DAG for the files.
 * AirflowConfig is used for the Config Values, while the RestTemplateBuilder is used
 *  to create the basic authentication header.
 */
@Service
public class AirflowTriggerService {

    private static final Logger LOG = LoggerFactory.getLogger(AirflowTriggerService.class);

    private final AirflowConfig props;
    private final RestTemplate rest;

    public AirflowTriggerService(AirflowConfig props, RestTemplateBuilder builder) {
        this.props = props;
        this.rest = builder
                .basicAuthentication(props.getAirflow().getUsername(), props.getAirflow().getPassword())
                .build();
    }

    /**
     * @param dagRunId  unique id for this run (we derive it from executionId)
     * @param conf DAG params: file_path, file_type, execution_id, jdbc_url, ...
     * @return raw response body from Airflow (JSON string)
     */
    public String triggerDagRun(String dagRunId, Map<String, Object> conf) {
        String url = props.getAirflow().getBaseUrl()
                + "/api/v1/dags/" + props.getAirflow().getDagId() + "/dagRuns";

        Map<String, Object> body = new HashMap<>();
        body.put("dag_run_id", dagRunId);
        body.put("conf", conf);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        LOG.info("Triggering DAG {} as run {}", props.getAirflow().getDagId(), dagRunId);
        ResponseEntity<String> resp =
                rest.postForEntity(url, new HttpEntity<>(body, headers), String.class);
        LOG.info("Airflow accepted run {} (HTTP {})", dagRunId, resp.getStatusCode());
        return resp.getBody();
    }
}
