BEGIN TRANSACTION;

CREATE TABLE spans (
    service_name VARCHAR,
    name VARCHAR,
    id VARCHAR PRIMARY KEY,
    parent_id VARCHAR,
    trace_id VARCHAR,
    kind VARCHAR,
    schema_url VARCHAR,
    resources map(VARCHAR, VARCHAR),
    scope_name VARCHAR,
    scope_version VARCHAR,
    start_timestamp TIMESTAMP,
    end_timestamp TIMESTAMP,
    flags UINTEGER,
    status_code VARCHAR,
    status_message VARCHAR,

    event_times TIMESTAMP[],
    event_names VARCHAR[],
    event_attrs map(VARCHAR, VARCHAR)[],

    link_trace_ids VARCHAR[],
    links_span_ids VARCHAR[],
    links_trace_states VARCHAR[],
    links_attrs map(VARCHAR, VARCHAR)[]
);

COMMIT;
