-- Please edit the sample below

CREATE STREAMING TABLE
    02_example_silver_audit
AS SELECT
    customer_name,
    order_number
FROM STREAM READ_FILES(
    "/databricks-datasets/retail-org/sales_orders/",
    format => "json",
    header => true
);