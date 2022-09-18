CREATE TABLE `hxb_dh_data`.`dws_basic_deposit_ratio_to_head` (
                                                                 `deposit_type` STRING COMMENT '存款类型 储蓄存款|公司存款',
                                                                 `head_last_year_avg_r` DECIMAL(30,6) COMMENT '总行下发去年日均',
                                                                 `head_curr_year_avg_r` DECIMAL(30,6) COMMENT '总行下发当前年日均',
                                                                 `wh_last_year_avg_r` DECIMAL(30,6) COMMENT '去年日均',
                                                                 `wh_curr_year_avg_r` DECIMAL(30,6) COMMENT '当前年日均',
                                                                 `stock_rate` DECIMAL(10,6) COMMENT '存量较总行比例',
                                                                 `increment_rate` DECIMAL(10,6) COMMENT '增量较总行比例',
                                                                 `etl_date` DATE)
    USING orc
PARTITIONED BY (etl_date)
COMMENT '基础型存款较总行比例'
TBLPROPERTIES (
  'transient_lastDdlTime' = '1663207036')
