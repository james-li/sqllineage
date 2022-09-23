CREATE TABLE `hxb_dh_data`.`loan_statistics` (
                                                 `branch_no` STRING,
                                                 `type2` INT,
                                                 `type3` INT,
                                                 `fenlei0` STRING,
                                                 `fenlei` STRING,
                                                 `balance` DECIMAL(30,2),
                                                 `etl_date` DATE)
    USING orc
PARTITIONED BY (etl_date)
TBLPROPERTIES (
  'transient_lastDdlTime' = '1646647694')
