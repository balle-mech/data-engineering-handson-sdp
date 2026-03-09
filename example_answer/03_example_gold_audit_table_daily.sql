CREATE OR REFRESH MATERIALIZED VIEW sdp_example_gold_audit_table_daily
(
  event_date DATE COMMENT '監査ログ基準日（日単位）',
  table_name STRING COMMENT 'アクセス対象テーブル名',
  audit_event_count LONG COMMENT 'テーブルへの総アクセス回数',
  distinct_user_count LONG COMMENT 'そのテーブルにアクセスしたユニークユーザー数',
  get_table_count LONG COMMENT 'getTable操作回数',
  command_submit_count LONG COMMENT 'commandSubmit操作回数'
)
COMMENT 'ゴールドレイヤ監査ログ集計'
AS
SELECT
  date(event_time) AS event_date,
  resource_name AS table_name,
  COUNT(*) AS audit_event_count,
  COUNT(DISTINCT email) AS distinct_user_count,
  SUM(CASE WHEN action_name = 'getTable' THEN 1 ELSE 0 END) AS get_table_count,
  SUM(CASE WHEN action_name = 'commandSubmit' THEN 1 ELSE 0 END) AS command_submit_count
FROM sdp_example_silver_audit
GROUP BY date(event_time), resource_name;