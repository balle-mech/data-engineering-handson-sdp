CREATE OR REFRESH STREAMING TABLE sdp_bronze_audit
COMMENT 'auditの生データ'
AS
SELECT
  event_id,
  event_time,
  action_name,
  resource_name,
  source_ip,
  user,
  request_params,
  current_timestamp AS _ingest_timestamp,
  _metadata.file_path AS _datasource
FROM
  STREAM read_files(
    '${audit_csv_path}',
    format => 'csv',
    header => true,
    quote => '"',
    escape => '"',
    multiline => true,
    inferSchema => false
);