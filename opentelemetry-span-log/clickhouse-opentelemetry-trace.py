#!/usr/bin/python

import argparse
import os
import subprocess
import uuid
import time

Q_QUERY_STATEMENT = ' \
SET opentelemetry_trace_processors=1; \
SET opentelemetry_start_trace_probability=1; \
{sql}\
'

Q_OPENTELEMETRY = ' \
SELECT \
    concat(substring(hostName(), length(hostName()), 1), leftPad(greatest(attribute[\'clickhouse.thread_id\'], attribute[\'thread_number\']), 5, \'0\')) AS group, \
    operation_name, \
    start_time_us, \
    finish_time_us, \
    sipHash64(operation_name) AS color, \
    attribute \
FROM system.opentelemetry_span_log \
WHERE (trace_id IN ( \
    SELECT trace_id \
    FROM system.opentelemetry_span_log \
    WHERE (attribute[\'clickhouse.query_id\']) = \'{id}\' \
)) AND (operation_name != \'query\') AND (operation_name NOT LIKE \'Query%\') \
ORDER BY \
    hostName() ASC, \
    group ASC, \
    parent_span_id ASC, \
    start_time_us ASC \
INTO OUTFILE \'{output_file_name}\' \
FORMAT JSON \
SETTINGS output_format_json_named_tuples_as_objects = 1 \
'

def main():
  parser = argparse.ArgumentParser(description='clickhouse pipeline trace svg graph throgh the pipeline trace log')
  parser.add_argument('--port', metavar='port', help='client port', default="9000")
  parser.add_argument('--client-location', metavar='CLIENT_LOCATION', help='ClickHosue client location', default=os.path.expanduser('~') + "/sandbox/ClickHouse/build/programs/clickhouse-client")
  parser.add_argument('--file-name', metavar='FILE_NAME', help='The output filename')
  parser.add_argument('sql', metavar='SQL', help='sql statement')
  args = parser.parse_args()
  
  if not args.file_name:
    print("output filename was required\n")
    parser.print_help()
    exit(1)

  if not args.sql:
    print("please input your sql statement\n")
    parser.print_help()
    exit(1)

  query_id=str(uuid.uuid1())

  subprocess.run([args.client_location,
                  "--port",
                  args.port,
                  "-nq",
                  Q_QUERY_STATEMENT.format(sql=args.sql),
                  "--query_id",
                  query_id])

  # wait for the statistics to be written
  time.sleep(5)
  
  subprocess.run([args.client_location,
                  "--port",
                  args.port,
                  "-q",
                  Q_OPENTELEMETRY.format(id=query_id, output_file_name=args.file_name)])
    

if __name__ == '__main__':
    main()
