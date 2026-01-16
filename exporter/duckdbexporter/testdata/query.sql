BEGIN TRANSACTION;

CREATE OR REPLACE TABLE spans (
    name VARCHAR,
    id VARCHAR primary key,
    parent_id VARCHAR,
    trace_id VARCHAR,
    kind UINTEGER,
    schema_url VARCHAR,
    resources map(VARCHAR, VARCHAR),
    resource_scope VARCHAR,
    start_timestamp TIMESTAMP,
    end_timestamp TIMESTAMP,
    flags UINTEGER,

    event_times TIMESTAMP[],
    event_names VARCHAR[],
    event_attrs map(VARCHAR, VARCHAR),

    link_trace_ids VARCHAR[],
    links_span_ids VARCHAR[],
    links_trace_states UINTEGER[],
    links_attrs map(VARCHAR, VARCHAR)
);

COMMIT;
