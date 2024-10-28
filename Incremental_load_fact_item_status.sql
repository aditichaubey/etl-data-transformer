BEGIN
DECLARE v_delta_offset timestamp;
DECLARE v_load_type string;
DECLARE v_inserted_record_count,v_deleted_record_count int64;
DECLARE v_start_timestamp timestamp;
SET v_start_timestamp = current_timestamp(); --Data Load Start Timestamp

SET V_LOAD_TYPE = '{{params.load_type}}';

SET
  v_delta_offset = (
  SELECT 
     CASE
        WHEN v_load_type = 'FULL_LOAD'
        THEN CAST('{{params.minimum_date}}' AS timestamp)
        ELSE
          IFNULL(date_Sub(MAX((CDC_TIMESTAMP_GCP)), INTERVAL {{params.delta_day_count}} DAY), CAST('{{params.minimum_date}}' AS timestamp))
        END
  FROM
    `{{params.target_project}}.{{params.target_dataset}}.fact_sales_trans_item_status_tbl`);

/*Generate a temporary staging table by COPYING from its main target table. This COPY process is fast and inherits all the configuration and metadata from the target table*/
CREATE OR REPLACE TABLE 
    `{{params.target_project}}.{{params.target_dataset}}.temp_staging_fact_sales_trans_item_status_tbl` 
COPY 
    `{{params.target_project}}.{{params.target_dataset}}.fact_sales_trans_item_status_tbl`;

/*Data from the most recent 60 days (Lookback Period) is being removed/deleted in case of DELETE-INSERT For FULL LOAD, complete table is truncated*/
IF v_load_type = 'FULL_LOAD' THEN
  TRUNCATE TABLE `{{params.target_project}}.{{params.target_dataset}}.temp_staging_fact_sales_trans_item_status_tbl`;
ELSE
  DELETE FROM `{{params.target_project}}.{{params.target_dataset}}.temp_staging_fact_sales_trans_item_status_tbl` 
  WHERE CDC_TIMESTAMP_GCP > v_delta_offset;
END IF;

/*Storing Delete Record Count using system variable*/
SET v_deleted_record_count = (select @@row_count); 

/*Data from the past 60 days, which was deleted by a previous operation, is being reinserted*/
INSERT INTO `{{params.target_project}}.{{params.target_dataset}}.temp_staging_fact_sales_trans_item_status_tbl`

WITH 
-- get most recent year for each currency conversion rate
years as (select max(cal_year) year, from_currency,
  FROM `{{params.source_project}}.COMMON_DMART.dim_currency_forex_rate_tbl` group by from_currency)

-- get most recent month for each currency conversion rate
,months as (
select max(cal_month) month, months.from_currency,
  FROM `{{params.source_project}}.COMMON_DMART.dim_currency_forex_rate_tbl` months, years where years.year = cal_year and years.from_currency = months.from_currency group by from_currency)

-- join months and years
,latest as(select months.from_currency, month, year from months, years where months.from_currency = years.from_currency)

-- table for most recent conversion rate for each currency
,status_forex_tbl as (SELECT forex.from_currency, forex.conversion_rate
  from `{{params.source_project}}.COMMON_DMART.dim_currency_forex_rate_tbl` forex, latest
  where forex.from_currency = latest.from_currency and latest.month = forex.cal_month and latest.year = forex.cal_year),

ITEM_STATUS AS (
SELECT 
  distinct(CONCAT(a.ITEM_STATUS_ID, '#', rcb.brand_id, '#', rcb.country_id)) fact_item_status_key,
  case 
    when rcb.country_id in (0,1,27) and a.STATUS_DATE is not null then a.STATUS_DATE 
    when rcb.country_id in (0,1,27) and a.STATUS_DATE is null then a.MODIFIED_DATE    
    else a.MODIFIED_DATE
  end as status_date_key, 
  rcb.country_id region_key, 
  concat(a.LINE_ITEM_STATUS,'#',rcb.brand_id,'#',rcb.country_id) item_status_key,
  a.LINE_ITEM_STATUS line_item_status,
  a.FULFILLER_ITEM_STATUS fulfiller_item_status,
  a.ITEM_STATUS_ID item_status_id, 
  a.CURRENT_STATUS current_status,
  rcb.brand_id brand_key,
  a.DC_CODE dc_code,
  a.TRANS_ORDER_ID trans_order_id,
  a.user_id user_key,
  a.CART_ID cart_id,
  a.SKU_BASE_ID sku_base_id,
  ifnull(pt.product_key,concat(a.product_code,'#',rcb.brand_id,'#',rcb.country_id)) product_key,
  a.ITEM_QUANTITY status_quantity,
  a.APPLIED_PRICE status_applied_amount,
  safe_divide(a.APPLIED_PRICE,forex.conversion_rate) status_applied_dollar_amount,
  a.DISCOUNT_AMOUNT status_applied_discount,
  safe_divide(a.DISCOUNT_AMOUNT,forex.conversion_rate) status_applied_dollar_discount,
  a.CANCEL_REASON_ID cancel_reason_id,
  cast(a.store_number as string) store_number,
  a.LATEST_STATUS latest_status,
  a.CDC_TIMESTAMP_GCP CDC_TIMESTAMP_GCP,
  --cast(null as int64) AS channel_key
  case 
	when rcb.brand_id = 41
	then 5
  else 1
  end as channel_key
FROM `{{params.source_project}}.{{params.source_dataset}}.sales_trans_item_status_tbl` a 
JOIN `{{params.source_project}}.COMMON_DMART.config_brand_region_tbl`rcb
			on rcb.dataset=a.dataset
JOIN status_forex_tbl forex 
			on forex.from_currency = rcb.country_from_curr
JOIN `{{params.source_project}}.{{params.source_dataset}}.sales_trans_item_tbl` t
			on t.TRANS_ORDER_ID = a.TRANS_ORDER_ID 
			and t.CART_ID = a.CART_ID 
			and t.DATASET = a.dataset 
			and t.SKU_BASE_ID = a.SKU_BASE_ID 
			and case when t.region_id is null THEN 1 WHEN t.region_id = rcb.country_id THEN 1 ELSE 0 END = 1 and upper(rcb.record_active)<>'NA'
			and case when t.brand_id  is null THEN 1 WHEN t.brand_id = rcb.brand_id    THEN 1 ELSE 0 END = 1 and upper(rcb.record_active)<>'NA'
			and t.USER_ID = a.USER_ID
      and case when a.shipment_id is null THEN 1 WHEN t.shipment_id is null then 1 WHEN a.shipment_id is not null and a.shipment_id = t.shipment_id THEN 1 ELSE 0 end = 1
LEFT JOIN `{{params.source_project}}.COMMON_DMART.dim_product_tbl`  pt
      ON  rcb.brand_id = pt.brand_id and rcb.country_id = pt.region_id and a.PRODUCT_CODE = pt.PRODUCT_CODE
WHERE a.CDC_TIMESTAMP_GCP>= v_delta_offset)
,
AGG_CHINA AS
(
WITH
  -- get most recent year FOR each currency conversion rate 
  years AS (
SELECT
	from_currency
	,conversion_rate
  , MAX(cal_year) year
FROM `{{params.source_project}}.{{params.common_dataset}}.dim_currency_forex_rate_tbl`
WHERE cal_month = EXTRACT(MONTH FROM CURRENT_DATE()) AND cal_year = cast(FORMAT_DATE('%y', CURRENT_DATE()) as int64)
	group by from_currency
	,conversion_rate
) 
  -- get most recent month FOR each currency conversion rate
  ,months AS (
  SELECT
    MAX(cal_month) month,
    months.from_currency,
  FROM
    `{{params.source_project}}.{{params.common_dataset}}.dim_currency_forex_rate_tbl` months,
    years
  WHERE
    years.year = cal_year
    AND years.from_currency = months.from_currency
  GROUP BY
    from_currency) 
    -- join months and years
, latest AS(
  SELECT
    months.from_currency,
    month,
    year
  FROM
    months,
    years
  WHERE
    months.from_currency = years.from_currency) -- TABLE FOR most recent conversion rate FOR each currency
    ,status_forex_tbl AS (
  SELECT
    forex.from_currency,
    forex.conversion_rate
  FROM
    `{{params.source_project}}.{{params.common_dataset}}.dim_currency_forex_rate_tbl` forex,
    latest
  WHERE
    forex.from_currency = latest.from_currency
    AND latest.month = forex.cal_month
    AND latest.year = forex.cal_year),
  brand_code AS (
  SELECT 
    LOWER(brand_code) brand_code,
    brand_id
FROM
 `{{params.source_project}}.{{params.common_dataset}}.dim_brand_tbl` ),


dedup_operation as (select distinct 
elc_brand_code,
elc_region_code,
country_code,
partition_date,
status_date,
item_amount,
item_quantity,
order_item_status_external_code,
order_item_status_internal_code,
transaction_source_code,
sku_base_id,
sku_reporting_code,
sku_type_code,
CONCAT(FORMAT_DATETIME('%Y-%m-%d', record_updated_date), ' 00:00:00') as record_updated_date,
item_tax_amount,
from `{{params.src_project}}.{{params.src_dataset}}.bir_shipped_sales_product_tbl` 
),

init_operation as
(
SELECT distinct
  GENERATE_UUID() AS fact_item_status_key,
  CAST(status_date AS DATETIME) AS status_date_key,
  dim_reg.region_id AS region_key,
  concat(st.item_status_id,'#',cast(rcb.BRAND_ID AS INT64),'#',dim_reg.region_id) item_status_key,
  st.item_status_id AS line_item_status,
  st.item_status_name AS fulfiller_item_status,
  st.item_status_id AS item_status_id,
  0 AS current_status,
  cast(rcb.BRAND_ID AS INT64) AS brand_key,
  0 AS dc_code,
  0 AS trans_order_id,
  0 AS user_key,
  0 AS cart_id,
  pr_sa.sku_base_id AS sku_base_id,
  ifnull(dm_pr.product_key,concat(dm_pr.PRODUCT_CODE,'#',rcb.brand_id,'#',rcb.country_id)) AS product_key,
  item_quantity AS status_quantity,
  item_amount AS status_applied_amount,
  SAFE_DIVIDE(item_amount,forex.conversion_rate) AS status_applied_dollar_amount,
  0 AS status_applied_discount,
  0 AS status_applied_dollar_discount,
  0 AS cancel_reason_id,
  '0' AS store_number,
  0 AS latest_status,
  cast(pr_sa.record_updated_date as timestamp) CDC_TIMESTAMP_GCP,
  CASE
      WHEN UPPER(pr_sa.transaction_source_code) = 'ECOM' THEN 1
      WHEN UPPER(pr_sa.transaction_source_code) = 'WECHAT' THEN 3
      WHEN UPPER(pr_sa.transaction_source_code) = 'WECHAT_CHANNEL' THEN 3
      WHEN UPPER(pr_sa.transaction_source_code) = 'XHS' THEN 6
      WHEN UPPER(pr_sa.transaction_source_code) = 'BAIDU' THEN 7
      WHEN UPPER(pr_sa.transaction_source_code) = 'TMALL' THEN 4
      WHEN UPPER(pr_sa.transaction_source_code) = 'FSS' THEN 2
    
    ELSE
    ch.channel_key
  END
    AS channel_key,
FROM
  dedup_operation pr_sa
LEFT JOIN
  status_forex_tbl forex
ON
  forex.from_currency = 'CNY'
LEFT JOIN
  brand_code br
ON
  br.brand_code = pr_sa.ELC_BRAND_CODE
LEFT JOIN
  `{{params.source_project}}.{{params.common_dataset}}.dim_product_tbl` dm_pr
ON
  dm_pr.SKU_BASE_ID = pr_sa.SKU_BASE_ID
  AND dm_pr.REGION_ID = 7
LEFT JOIN
  `{{params.source_project}}.{{params.common_dataset}}.dim_region_tbl` dim_reg
ON
  dim_reg.region_id = 7
LEFT JOIN
  `{{params.source_project}}.{{params.common_dataset}}.config_brand_region_tbl`rcb
ON
    rcb.brand_id = br.BRAND_ID and rcb.country_id = 7 and record_active = 'A'
LEFT JOIN
    `{{params.source_project}}.{{params.common_dataset}}.dim_channel_tbl` ch
  ON
    UPPER(ch.channel_name) = UPPER(pr_sa.transaction_source_code) 
LEFT JOIN
  `{{params.source_project}}.{{params.source_dataset}}.dim_item_status_tbl` st
ON
  UPPER(REGEXP_REPLACE(regexp_replace(st.item_status_name,
        '[0-9]',
        ''), r'[\(\)\d]+', '')) = UPPER(pr_sa.ORDER_ITEM_STATUS_INTERNAL_CODE)
  AND st.region_id = 7
  AND st.brand_id = cast(br.BRAND_ID AS INT64)
  WHERE cast(pr_sa.record_updated_date as timestamp) >= v_delta_offset
  AND order_item_status_internal_code = 'shipped'
  AND sku_type_code = 'se'  
) select * from init_operation
)
,
TOMFORD_STATUS AS (
SELECT 
  distinct(CONCAT(fulfillment_line_uuid, '#', rcb.brand_id, '#', rcb.country_id)) fact_item_status_key,
  ifnull(fulfilment_datetime, line_item_datetime) status_date_key,  -- if not fulfilled then pending only line item date available
  rcb.country_id region_key, 
  concat(ist.item_status_id,'#',rcb.brand_id,'#',rcb.country_id) item_status_key,
  ist.item_status_id line_item_status,
  fulfilment_status fulfiller_item_status,
  fulfillment_line_id item_status_id, 
  cast(a.LATEST_STATUS as int64) current_status,
  rcb.brand_id brand_key,
  Case 
  when rcb.country_id = 0 and UPPER(fulfilment_status) = "SHIPPED" then 210
  when rcb.country_id = 1 and UPPER(fulfilment_status) = "SHIPPED" then 845
  else Null END as dc_code, -- We currently have data for two countries for Tom Ford: the US and the UK.
  order_id trans_order_id,
  user_id user_key,
  99999 as cart_id, -- Currently providing the dummy value, need to bring in the cart_id from landing
  pt.sku_base_id sku_base_id,
  ifnull(pt.product_key,concat(a.sku_id,'#',rcb.brand_id,'#',rcb.country_id)) product_key,
  a.status_quantity status_quantity,
  a.status_applied_amount status_applied_amount,
  safe_divide(a.status_applied_amount,forex.conversion_rate) status_applied_dollar_amount,
  a.status_applied_discount status_applied_discount,
  safe_divide(a.status_applied_discount,forex.conversion_rate) status_applied_dollar_discount,
  CAST(item_cancel_reason_id as INT64) cancel_reason_id, -- in dim item cancel reason id is string
  '' store_number,
  cast(a.LATEST_STATUS as int64) latest_status,
  timestamp(a.line_item_datetime) CDC_TIMESTAMP_GCP, --- need to be timestamp
  channel_key AS channel_key
FROM `{{params.source_project}}.CKR_TF_SALES_UNION.st_tf_sales_trans_item_status_tbl` a 
JOIN `{{params.source_project}}.{{params.common_dataset}}.config_brand_region_tbl`rcb on rcb.brand_id=a.brand_id and rcb.country_id=a.region_id
JOIN status_forex_tbl forex  on forex.from_currency = rcb.country_from_curr
LEFT JOIN `{{params.source_project}}.{{params.source_dataset}}.dim_item_status_tbl` ist on ist.item_status_name=fulfilment_status and ist.brand_id=rcb.brand_id and ist.region_id=rcb.country_id 
LEFT JOIN `{{params.source_project}}.{{params.common_dataset}}.dim_product_tbl` pt ON rcb.brand_id = pt.brand_id and rcb.country_id = pt.region_id and a.sku_id = pt.PRODUCT_CODE
LEFT JOIN `{{params.source_project}}.{{params.source_dataset}}.dim_item_cancel_reason_tbl` cr on cr.item_cancel_reason_code = a.reason_code
WHERE timestamp(a.line_item_datetime) >= v_delta_offset
)

SELECT * FROM ITEM_STATUS
UNION ALL
SELECT * FROM AGG_CHINA
UNION ALL
SELECT * FROM TOMFORD_STATUS;

/*Storing Insert Record Count using system variable*/
SET v_inserted_record_count = (select @@row_count);

/*Data from the past 60 days is being inserted into the staging table. The entire staging table is then copied to the target table. This copying process is swift and inherits all the configurations and metadata from the staging table*/

CREATE OR REPLACE TABLE
    `{{params.target_project}}.{{params.target_dataset}}.fact_sales_trans_item_status_tbl` 
COPY
    `{{params.target_project}}.{{params.target_dataset}}.temp_staging_fact_sales_trans_item_status_tbl`;

/*Since the staging table has fulfilled its purpose, there is no need to retain it. Therefore, the table will be dropped, and it will be recreated in the next run*/
DROP TABLE 
  `{{params.target_project}}.{{params.target_dataset}}.temp_staging_fact_sales_trans_item_status_tbl`;

/*For the purpose of logging, the entire AUDIT information is being stored in a table called elc_audit_insert_tbl using below procedure call*/
CALL `{{params.target_project}}.{{params.common_dataset}}.proc_elc_audit_insert`(
    '{{params.dag_name}}',
    '{{params.target_project}}', 
    '{{params.target_dataset}}', 
    'fact_sales_trans_item_status_tbl', 
    v_start_timestamp, 
    'DW_FACT', 
    v_load_type, 
    v_inserted_record_count, 
    v_deleted_record_count, 
    'CONCAT(item_status_key, line_item_status, latest_status, trans_order_id, cart_id)');
END;