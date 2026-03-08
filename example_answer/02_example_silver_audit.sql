-- ============================================================================= 
-- エクスペクテーションの追加 
-- ============================================================================= 
-- 
-- 【このファイルについて】 
-- 演習2のパイプラインにデータ品質チェック（エクスペクテーション）を追加します。 
-- 既存のパイプラインを編集するか、新しいパイプラインを作成してください。 
-- 
-- 【エクスペクテーションの3つのモード】 
-- - EXPECT (条件) : 警告のみ（データは保持） 
-- - EXPECT (条件) ON VIOLATION DROP ROW : 違反行を除外 
-- - EXPECT (条件) ON VIOLATION FAIL UPDATE : パイプライン停止 
-- =============================================================================

CREATE TEMPORARY VIEW sdp_audit_cleaned AS
SELECT
  event_id,
  CAST(event_time AS TIMESTAMP) AS event_time,
  -- trim(str)で両端空白を除去、trim(LEADING FROM str)で先頭のみ、trim(TRAILING FROM str)で末尾のみ
  trim(action_name) AS action_name,
  trim(resource_name) AS resource_name,
  source_ip,
  user,
  -- user JSONを展開し、emailとnameに分割
  trim(from_json(user, 'STRUCT<email:STRING,name:STRING>').email) AS email,
  from_json(user, 'STRUCT<email:STRING,name:STRING>').name AS name,
  request_params,
  from_json(request_params, 'STRUCT<full_name_arg:STRING>').full_name_arg AS full_name_arg,
  _ingest_timestamp,
  _datasource
FROM STREAM(sdp_bronze_audit);

CREATE OR REFRESH STREAMING TABLE sdp_silver_audit (
  CONSTRAINT valid_event_id EXPECT (event_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_event_time EXPECT (event_time IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_action_name EXPECT (action_name IS NOT NULL AND length(action_name) > 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_email EXPECT (email IS NOT NULL AND length(trim(email)) > 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_source_ip EXPECT (source_ip IS NOT NULL)
);

-- event_id が重複した場合、_ingest_timestamp が最新ものを残す
CREATE FLOW scd_type1_bronze_to_silver
COMMENT 'auditのクレンジング済みデータ'
AS
AUTO CDC INTO sdp_silver_audit
FROM STREAM(sdp_audit_cleaned)
KEYS (event_id)
SEQUENCE BY _ingest_timestamp
STORED AS SCD TYPE 1;