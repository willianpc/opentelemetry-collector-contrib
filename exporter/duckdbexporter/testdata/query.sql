-- TODO: events table with event_name, event_timestamp, event_attributes(map varchar, varchar)

BEGIN TRANSACTION;
DROP TABLE IF EXISTS spans;

CREATE TABLE spans (
    span_name varchar,
    span_id varchar,
    parent_id varchar,
    trace_id varchar,
    span_kind uinteger,
    schema_url varchar,
    resources map(varchar, varchar),
    resource_scope varchar,
    start_timestamp timestamp,
    end_timestamp timestamp,
    flags uinteger,
);

COMMIT;
