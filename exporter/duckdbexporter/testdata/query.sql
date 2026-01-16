-- TODO: events table with event_name, event_timestamp, event_attributes(map varchar, varchar)

BEGIN TRANSACTION;

-- DROP TABLE IF EXISTS spans;

CREATE OR REPLACE TABLE spans (
    name varchar,
    id varchar primary key,
    parent_id varchar,
    trace_id varchar,
    kind uinteger,
    schema_url varchar,
    resources map(varchar, varchar),
    resource_scope varchar,
    start_timestamp timestamp,
    end_timestamp timestamp,
    flags uinteger,
);

CREATE OR REPLACE TABLE events (
    span_id varchar,
    foreign key(span_id) references spans(id),
    name varchar,
    attributes map(varchar, varchar)
);

COMMIT;
