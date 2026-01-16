# Traces Table

Resource
- Schema URL
- Resource Attributes (map)
- Spans
  - scope: varchar
  - Name: request handler - /foo
  - Kind: Internal
  - ID: 9878a0481299f820
  - parent_ID: a8646f5eb13ab2e2
  - Trace_ID: 6107b2f04ff5a93f26f532109ae35fe7
  - Span-attributes (map)

Resource table? Maybe, but need link to spans. also resource mau change. eg: hostname changes or resource values change.
Probably best bet is to make Resources as part of each span

Better is probably span table, kind of like this:

Span table:
- scope: varchar (from resource)
- Name: request handler - /foo
- Kind: Internal
- ID: 9878a0481299f820
- parent_ID: a8646f5eb13ab2e2
- Trace_ID: 6107b2f04ff5a93f26f532109ae35fe7
- Span-attributes (map)
- Resource-attributes (map from resources, varchar, varchar)

Span:
 - schema URL (from Resource, varchar)
 - resources: (map)
 - resource-scope: varchar
 - Name: request handler - /foo
 - Kind: Internal
 - span_ID: 9878a0481299f820
 - parent_ID: a8646f5eb13ab2e2
 - Trace_ID: 6107b2f04ff5a93f26f532109ae35fe7
 - start timestamp
 - end timestamp
 - events (must be a new table linked by span id)
   - event name
   - event timestamp
   - event attributes (map)
 - links (span id) (list, span id may be sufficient, may become a table, but probably not, cause it is a complete span already)
 - flags (int)
 - status

