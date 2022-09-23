create table if not exists hxb_dh_data_dws.dws_staff_dep_loan_sum
(
    absorb_code              string comment '员工编号',
    user_name                string comment '员工名称',
    corp_gen_deps_q_avg      decimal(30, 6) comment '对公存款季日均',            --
    corp_gen_deps_y_avg      decimal(30, 6) comment '对公存款年日均',            --
    per_gen_deps_q_avg       decimal(30, 6) comment '储蓄存款季日均',            --
    per_gen_deps_y_avg       decimal(30, 6) comment '储蓄存款年日均',            --
    per_margin_deps_q_avg    decimal(30, 6) comment '储蓄保证金存款季日均',         --
    per_margin_deps_y_avg    decimal(30, 6) comment '储蓄保证金存款年日均',         --
    per_hui_surplus_q_avg    decimal(30, 6) comment '个人慧盈季日均',            --
    per_hui_surplus_y_avg    decimal(30, 6) comment '个人慧盈年日均',            --
    per_mm_pas_q_avg         decimal(30, 6) comment '个人理财在途季日均',          --
    per_mm_pas_y_avg         decimal(30, 6) comment '个人理财在途年日均',          --
    per_mm_pas_q_ir          decimal(30, 6) comment '理财在途付息率(季度)',        --

    per_no_loan_deps_q_avg   decimal(30, 6) comment '储蓄无贷户纯存款季度日均',       --
    per_no_loan_q_ir         decimal(10, 2) comment '储蓄无贷户纯存款付息率(季度)',    --
    hxb_per_no_loan_q_ir     decimal(10, 2) comment '全行储蓄无贷户纯存款付息率(季度)',  --

    corp_no_loan_deps_q_avg  decimal(30, 6) comment '对公无贷户纯存款季度日均',       --
    corp_no_loan_q_ir        decimal(10, 2) comment '对公无贷户纯存款付息率(季度)',    --

    no_loan_gen_deps_q_avg   decimal(30, 6) comment '一般性无贷户纯存款季度日均',      --
    no_loan_gen_q_ir         decimal(10, 2) comment '一般性无贷户纯存款付息率(季度)',   --
    hxb_no_loan_q_ir         decimal(10, 2) comment '全行一般性无贷户纯存款付息率(季度)', --

    compr_deps_avg_q         decimal(30, 6) comment '综合存款季度日均',
    compr_deps_avg_y         decimal(30, 6) comment '综合存款年度日均',
    inc_corp_gen_deps_avg_q  decimal(30, 6) comment '净增全口径对公存款日均季度',      --
    inc_corp_gen_deps_avg_y  decimal(30, 6) comment '净增全口径对公存款日均年度',      --
    inc_corp_loan_credit_bal decimal(30, 6) comment '净增对公贷款',             --
    corp_loan_credit_bal     decimal(30, 6) comment '对公贷款用信余额 borm',      --
    inc_inclus_dbl_inc_q     decimal(30, 6) comment '新增普惠条线两增贷款(季)',
    inc_inclus_dbl_inc_y     decimal(30, 6) comment '新增普惠条线两增贷款(年）',
    inc_inclus_loan_q        decimal(30, 6) comment '新增普惠条线贷款(季)',
    inc_inclus_loan_y        decimal(30, 6) comment '新增普惠条线贷款(年）',
    inclus_dbl_inc           decimal(30, 6) comment '普惠条线两增贷款',           -- 余额
    inclus_loan_bal          decimal(30, 6) comment '普惠贷款规模',-- 余额
    inclus_loan_cust         decimal(30, 6) comment '普惠贷款客户数',
    inc_dbl_inc_cust_q       decimal(30, 6) comment '净增普惠条线两增客户数(季）',
    inc_dbl_inc_cust_y       decimal(30, 6) comment '净增普惠条线两增客户数（年）',
    inc_inclus_loan_cust_q   decimal(30, 6) comment '净增普惠条线贷款客户数（季）',
    inc_inclus_loan_cust_y   decimal(30, 6) comment '净增普惠条线贷款客户数（年）',
    less_fifty_million_cust  decimal(30, 6) comment '小于5000万对公有效户数',      --
    fifty_million_cust       decimal(30, 6) comment '5000万对公有效户数',        --
    one_hundred_million_cust decimal(30, 6) comment '1亿对公有效户数',           --
    per_loan_delivery_q      decimal(30, 6) comment '个贷当季投放',
    per_loan_delivery_y      decimal(30, 6) comment '个贷累计投放',
    inc_fin_avg              decimal(30, 6) comment '净增理财日均'              --
    ) using orc partitioned by (etl_date date) comment '各经营机构存款汇总表-对公按吸存关系-对私不按吸存关系';
