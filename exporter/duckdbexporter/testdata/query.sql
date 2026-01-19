BEGIN TRANSACTION;

CREATE TABLE otel_traces (
	service_name VARCHAR,
    name VARCHAR,
    span_id VARCHAR,
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

CREATE TABLE otel_logs (
    timestamp TIMESTAMP,
    trace_id VARCHAR,
    span_id VARCHAR,
    flags UBIGINT,
    severity_text VARCHAR,
    severity_number UBIGINT,
    service_name VARCHAR,
    body VARCHAR,
    res_url VARCHAR,
    res_attrs map(VARCHAR, VARCHAR),
    scope_url VARCHAR,
    scope_name VARCHAR,
    scopeVersion VARCHAR,
    scope_attrs map(VARCHAR, VARCHAR),
    log_attrs map(VARCHAR, VARCHAR),
    event_name VARCHAR
);

COMMIT;
