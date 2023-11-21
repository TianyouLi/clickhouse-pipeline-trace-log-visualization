= Example =

```
rm output.json;python clickhouse-opentelemetry-trace.py --file-name output.json --port 9002 "select count(*) from test.hits"
```
