
with tmp_corp_loan_borm as (
    with tmp_corp_last_loan_bal as (
        select absorb_code, sum(loan_bal_r) corp_loan_credit_bal
        from hxb_dh_data_dwm.dwm_acct_loan_absorb_sum
        where etl_date = concat(cast(year('${date}') - 1 as string), '-12-31')
          and CUSTOMER_TYPE = '02' -- 对公贷款余额
        group by absorb_code
    ),
         tmp_corp_loan_bal as (
             select absorb_code, sum(loan_bal_r) corp_loan_credit_bal
             from hxb_dh_data_dwm.dwm_acct_loan_absorb_sum
             where etl_date = '${date}'
               and CUSTOMER_TYPE = '02' -- 对公贷款余额
             group by absorb_code
         )
    select coalesce(a.absorb_code, b.absorb_code)                                absorb_code,
           ifnull(a.corp_loan_credit_bal, 0) - ifnull(b.corp_loan_credit_bal, 0) inc_corp_loan_credit_bal,
           ifnull(a.corp_loan_credit_bal, 0)                                     corp_loan_credit_bal
    from tmp_corp_loan_bal a
             full outer join tmp_corp_last_loan_bal b on a.absorb_code = b.absorb_code
),
     tmp_mgr_fin as ( ----------------理财-----------------
         with tmp_mgr_last_fin as (
             select absorb_code,
                    sum(curr_bal_avg_y) curr_bal_avg_y,
                    sum(curr_bal_avg_q) curr_bal_avg_q
             from hxb_dh_data_dwm.dwm_cust_vol_absorb_avg
             where etl_date = concat(cast(year('${date}') - 1 as string), '-12-31')
             group by absorb_code
             order by curr_bal_avg_q, curr_bal_avg_y desc
         ),
              tmp_mgr_fin as (
                  select absorb_code,
                         sum(curr_bal_avg_y) curr_bal_avg_y,
                         sum(curr_bal_avg_q) curr_bal_avg_q
                  from hxb_dh_data_dwm.dwm_cust_vol_absorb_avg
                  where etl_date = '${date}'
                  group by absorb_code
              ),
              tmp_mgr_hui_surplus as ( ------------慧盈理财-----------------
                  select absorb_code,
                         sum(curr_bal_avg_y) per_hui_surplus_y_avg,
                         sum(curr_bal_avg_q) per_hui_surplus_q_avg
                  from hxb_dh_data_dwm.dwm_cust_vol_absorb_avg
                  where etl_date = '${date}'
                    and cust_type = '01'
                    and PROD_NAME regexp '慧盈'
group by absorb_code
    )
select coalesce(a.absorb_code, b.absorb_code, c.absorb_code)     absorb_code,
       ifnull(a.curr_bal_avg_y, 0) - ifnull(b.curr_bal_avg_y, 0) inc_fin_avg_y,
       ifnull(a.curr_bal_avg_q, 0) - ifnull(b.curr_bal_avg_q, 0) inc_fin_avg_q,
       ifnull(c.per_hui_surplus_y_avg, 0)                        per_hui_surplus_y_avg,
       ifnull(c.per_hui_surplus_q_avg, 0)                        per_hui_surplus_q_avg
from tmp_mgr_fin a
         full outer join tmp_mgr_last_fin b on a.absorb_code = b.absorb_code
         full outer join tmp_mgr_hui_surplus c
                         on coalesce(a.absorb_code, b.absorb_code) = c.absorb_code
    ),
     tmp_mgr_deps as ( ------------存款-----------------
         with tmp_mgr_corp_gen_deps_avg as ( --对公全口径存款
             with tmp_mgr_last_corp_gen_deps_avg as ( -- 上年对公全口径存款
                 select absorb_code,
                        sum(bal_r_avg_q) last_corp_gen_deps_q_avg,
                        sum(bal_r_avg_y) last_corp_gen_deps_y_avg
                 from hxb_dh_data_dwm.dwm_acct_absorb_deposit_details_d
                 where etl_date = concat(cast(year('${date}') - 1 as string), '-12-31')
                   and is_general = 'Y'
                   and customer_type = 'C' -- 对公
                   --and a.general_type regexp '对公|其他'
                 group by absorb_code
             ),
                  tmp_mgr_curr_corp_gen_deps_avg as ( -- 对公全口径存款
                      select absorb_code,
                             sum(bal_r_avg_q) corp_gen_deps_q_avg,
                             sum(bal_r_avg_y) corp_gen_deps_y_avg
                      from hxb_dh_data_dwm.dwm_acct_absorb_deposit_details_d
                      where etl_date = '${date}'
                        and is_general = 'Y'
                        and customer_type = 'C' -- 对公
                        --and a.general_type regexp '对公|其他'
                      group by absorb_code
                  ),
                  tmp_mgr_curr_corp_cust_num as ( -- 对公有效户 5千万 一亿
                      with tmp_cust_q_avg as (
                          select customer_no,
                                 sum(ifnull(bal_r_avg_q, 0)) corp_cust_q_avg
                          from hxb_dh_data_dwm.dwm_acct_absorb_deposit_details_d
                          where etl_date = '${date}'
                            and is_general = 'Y'
                            and customer_type = 'C' -- 对公
                            --and a.general_type regexp '对公|其他'
                          group by customer_no
                          having corp_cust_q_avg > 0
                      ),
                           tmp_cust_num_detail as (
                               select a.absorb_code,
                                      a.customer_no,
                                      case
                                          when b.corp_cust_q_avg < 50000000 -- 小于5千万客户数
                                              then sum(a.bal_r_avg_q) / ifnull(b.corp_cust_q_avg, 1)
                                          else 0 end less_fifty_million_cust,
                                      case
                                          when b.corp_cust_q_avg >= 50000000 and b.corp_cust_q_avg < 100000000 -- 大于5千万客户数
                                              then sum(a.bal_r_avg_q) / ifnull(b.corp_cust_q_avg, 1)
                                          else 0 end fifty_million_cust,
                                      case
                                          when b.corp_cust_q_avg >= 100000000 -- 大于1亿客户数
                                              then sum(a.bal_r_avg_q) / ifnull(b.corp_cust_q_avg, 1)
                                          else 0 end one_hundred_million_cust,
                                      b.corp_cust_q_avg
                               from hxb_dh_data_dwm.dwm_acct_absorb_deposit_details_d a
                                        left join tmp_cust_q_avg b on a.customer_no = b.customer_no
                               where etl_date = '${date}'
                                 and is_general = 'Y'
                                 and customer_type = 'C' -- 对公
                                 --and a.general_type regexp '对公|其他'
                               group by a.absorb_code, a.customer_no, b.corp_cust_q_avg
                               order by b.corp_cust_q_avg desc
                           )
                      select absorb_code,
                             sum(less_fifty_million_cust)  less_fifty_million_cust,
                             sum(fifty_million_cust)       fifty_million_cust,
                             sum(one_hundred_million_cust) one_hundred_million_cust
                      from tmp_cust_num_detail
                      group by absorb_code
                  )
             select coalesce(a.absorb_code, b.absorb_code, c.absorb_code)                    absorb_code,
                    ifnull(a.corp_gen_deps_q_avg, 0)                                         corp_gen_deps_q_avg,
                    ifnull(a.corp_gen_deps_y_avg, 0)                                         corp_gen_deps_y_avg,
                    ifnull(a.corp_gen_deps_q_avg, 0) - ifnull(b.last_corp_gen_deps_q_avg, 0) inc_corp_gen_deps_q_avg,
                    ifnull(a.corp_gen_deps_y_avg, 0) - ifnull(b.last_corp_gen_deps_y_avg, 0) inc_corp_gen_deps_y_avg,
                    ifnull(less_fifty_million_cust, 0)                                       less_fifty_million_cust,
                    ifnull(fifty_million_cust, 0)                                            fifty_million_cust,
                    ifnull(one_hundred_million_cust, 0)                                      one_hundred_million_cust
             from tmp_mgr_curr_corp_gen_deps_avg a
                      full outer join tmp_mgr_last_corp_gen_deps_avg b
                                      on a.absorb_code = b.absorb_code
                      full outer join tmp_mgr_curr_corp_cust_num c
                                      on coalesce(a.absorb_code, b.absorb_code) = c.absorb_code
         ),
              tmp_mgr_per_gen_deps_avg as ( -- 储蓄存款
                  with tmp_mgr_curr_per_gen_deps_avg as (
                      select absorb_code,
                             sum(bal_r_avg_q) per_gen_deps_q_avg,
                             sum(bal_r_avg_y) per_gen_deps_y_avg
                      from hxb_dh_data_dwm.dwm_acct_absorb_deposit_details_d
                      where etl_date = '${date}'
                        and is_general = 'Y'
                        and customer_type = 'P'
                        -- 个人
                        --and a.general_type regexp '对公|其他'
                      group by absorb_code
                  ),
                       tmp_mgr_per_margin_deps_avg as ( -- 储蓄保证金存款
                           select absorb_code,
                                  sum(bal_r_avg_q) per_margin_deps_q_avg,
                                  sum(bal_r_avg_y) per_margin_deps_y_avg
                           from hxb_dh_data_dwm.dwm_acct_absorb_deposit_details_d
                           where etl_date = ' ${date}'
                             and is_general = 'Y'
                             and customer_type = 'P'       -- 个人
                             and class_code regexp '^2104' -- 保证金
                           group by absorb_code
                       )
                  select ifnull(a.absorb_code, b.absorb_code) absorb_code,
                         ifnull(a.per_gen_deps_q_avg, 0)      per_gen_deps_q_avg,
                         ifnull(a.per_gen_deps_y_avg, 0)      per_gen_deps_y_avg,
                         ifnull(b.per_margin_deps_q_avg, 0)   per_margin_deps_q_avg,
                         ifnull(b.per_margin_deps_y_avg, 0)   per_margin_deps_y_avg
                  from tmp_mgr_curr_per_gen_deps_avg a
                           full outer join tmp_mgr_per_margin_deps_avg b
                                           on a.absorb_code = b.absorb_code
              ),
              tmp_mgr_pure_deps_avg as (
                  with tmp_mgr_per_pure_deps_avg as ( -- 储蓄纯存款
                      with tmp_cust_has_loan as ( -- 用于计算纯存款，剔除有贷户(季日均不为0)
                          select CUSTOMER_NO,
                                 sum(quarter_avg_loan_bal_r) quarter_avg_loan_bal_r
                          from hxb_dh_data_dwm.dwm_acct_loan_absorb_sum
                          where etl_date = '${date}'
                            and CUSTOMER_TYPE = '01'
                          group by CUSTOMER_NO
                          having quarter_avg_loan_bal_r > 0
                      )
                      select absorb_code,
                             sum(ifnull(bal_r_avg_q, 0)) per_no_loan_deps_q_avg,
                             --sum(ifnull(bal_r_avg_y, 0))                                    per_no_loan_deps_y_avg,
                             sum(ifnull(bal_r_avg_q, 0) * ifnull(cr_store_rate, 0)) /
                             sum(ifnull(bal_r_avg_q, 1)) per_no_loan_q_ir
                      from hxb_dh_data_dwm.dwm_acct_absorb_deposit_details_d a
                               left join tmp_cust_has_loan b
                                         on a.customer_no = b.CUSTOMER_NO
                      where etl_date = '${date}'
                        and is_general = 'Y'
                        and customer_type = 'P'   -- 个人
                        and deps_label = '非保证金'
                        and b.CUSTOMER_NO is null -- 剔除有贷款账户
                        and curr_status = '00'
                      group by absorb_code
                  ),
                       tmp_mgr_corp_pure_deps_avg as ( -- 对公纯存款
                           with tmp_cust_has_loan as ( -- 用于计算纯存款，剔除有贷户(季日均不为0)
                               select CUSTOMER_NO,
                                      sum(quarter_avg_loan_bal_r) quarter_avg_loan_bal_r
                               from hxb_dh_data_dwm.dwm_acct_loan_absorb_sum
                               where etl_date = '${date}'
                                 and CUSTOMER_TYPE = '02'
                               group by CUSTOMER_NO
                               having quarter_avg_loan_bal_r > 0
                           )
                           select absorb_code,
                                  sum(ifnull(bal_r_avg_q, 0))                         corp_no_loan_deps_q_avg,
                                  -- sum(ifnull(bal_r_avg_y, 0))                                    corp_no_loan_deps_y_avg,
                                  sum(ifnull(bal_r_avg_q, 0) * ifnull(cr_store_rate, 0)) /
                                  sum(if(ifnull(bal_r_avg_q, 0) = 0, 1, bal_r_avg_q)) corp_no_loan_q_ir
                           from hxb_dh_data_dwm.dwm_acct_absorb_deposit_details_d a
                                    left join tmp_cust_has_loan b
                                              on a.customer_no = b.CUSTOMER_NO
                           where etl_date = '${date}'
                             and is_general = 'Y'
                             and customer_type = 'C'   -- 公司
                             and deps_label = '非保证金'
                             and b.CUSTOMER_NO is null -- 剔除有贷款账户
                             and curr_status = '00'
                           group by absorb_code
                       ),
                       tmp_mgr_fin_in_pas as ( ------------理财在途-----------------
                           select absorb_code,
                                  sum(bal_q_avg) per_mm_pas_q_avg,
                                  sum(bal_y_avg) per_mm_pas_y_avg,
                                  0.3            per_mm_pas_q_ir
                           from hxb_dh_data_dwm.dwm_fin_in_pas_absorb_sum
                           where etl_date = '${date}'
                           group by absorb_code
                       )
                  select absorb_code,
                         per_mm_pas_q_avg,
                         per_mm_pas_y_avg,
                         per_mm_pas_q_ir,
                         hxb_per_no_loan_q_ir,
                         hxb_corp_no_loan_q_ir,
                         hxb_no_loan_q_ir,
                         case
                             when per_pure_q_ir > hxb_per_no_loan_q_ir
                                 then per_pure_deps_q_avg * hxb_per_no_loan_q_ir / per_pure_q_ir
                             else per_pure_deps_q_avg end per_pure_deps_q_avg,
                         per_no_loan_q_ir,
                         corp_no_loan_deps_q_avg,
                         corp_no_loan_q_ir,
                         case
                             when gen_pure_q_ir > hxb_no_loan_q_ir
                                 then gen_pure_deps_q_avg * hxb_no_loan_q_ir / gen_pure_q_ir
                             else gen_pure_deps_q_avg end gen_pure_deps_q_avg,
                         gen_pure_q_ir
                  from (
                           select absorb_code,
                                  per_no_loan_deps_q_avg,
                                  per_no_loan_q_ir,
                                  corp_no_loan_deps_q_avg,
                                  corp_no_loan_q_ir,
                                  per_mm_pas_q_avg,
                                  per_mm_pas_y_avg,
                                  per_mm_pas_q_ir,
                                  hxb_per_no_loan_q_ir,
                                  hxb_corp_no_loan_q_ir,
                                  hxb_no_loan_q_ir,
                                  per_no_loan_deps_q_avg + per_mm_pas_q_avg                           per_pure_deps_q_avg,
                                  per_no_loan_deps_q_avg + per_mm_pas_q_avg + corp_no_loan_deps_q_avg gen_pure_deps_q_avg,
                                  (per_no_loan_deps_q_avg * per_no_loan_q_ir + per_mm_pas_q_avg * per_mm_pas_q_ir) /
                                  if((per_no_loan_deps_q_avg + per_mm_pas_q_avg) = 0, 1,
                                     per_no_loan_deps_q_avg +
                                     per_mm_pas_q_avg)                                                per_pure_q_ir, -- 储蓄纯存款付息率 避免除0
                                  (per_no_loan_deps_q_avg * per_no_loan_q_ir + per_mm_pas_q_avg * per_mm_pas_q_ir +
                                   corp_no_loan_deps_q_avg * corp_no_loan_q_ir) /
                                  if((per_no_loan_deps_q_avg + per_mm_pas_q_avg + corp_no_loan_deps_q_avg) = 0, 1,
                                     per_no_loan_deps_q_avg + per_mm_pas_q_avg +
                                     corp_no_loan_deps_q_avg)                                         gen_pure_q_ir  -- 一般性纯存款付息率 避免除0
                           from (
                                    select coalesce(a.absorb_code, b.absorb_code, c.absorb_code) absorb_code,
                                           ifnull(a.per_no_loan_deps_q_avg, 0)                   per_no_loan_deps_q_avg,
                                           ifnull(a.per_no_loan_q_ir, 0)                         per_no_loan_q_ir,
                                           ifnull(b.corp_no_loan_deps_q_avg, 0)                  corp_no_loan_deps_q_avg,
                                           ifnull(b.corp_no_loan_q_ir, 0)                        corp_no_loan_q_ir,
                                           ifnull(c.per_mm_pas_q_avg, 0)                         per_mm_pas_q_avg,
                                           ifnull(c.per_mm_pas_y_avg, 0)                         per_mm_pas_y_avg,
                                           ifnull(c.per_mm_pas_q_ir, 0)                          per_mm_pas_q_ir,
                                           d.hxb_per_no_loan_q_ir,
                                           d.hxb_corp_no_loan_q_ir,
                                           d.hxb_no_loan_q_ir
                                    from tmp_mgr_per_pure_deps_avg a
                                             full outer join tmp_mgr_corp_pure_deps_avg b
                                                             on a.absorb_code = b.absorb_code
                                             full outer join tmp_mgr_fin_in_pas c
                                                             on coalesce(a.absorb_code, b.absorb_code) = c.absorb_code
                                             full outer join hxb_dh_data_dim.dim_hxb_deps_interest_rate d
                                                             on d.year = year('${date}') and d.month = month('${date}')
                                )
                       )
              )
         select coalesce(a.absorb_code, b.absorb_code, c.absorb_code) absorb_code,
                ifnull(a.corp_gen_deps_q_avg, 0)                      corp_gen_deps_q_avg,
                ifnull(a.corp_gen_deps_y_avg, 0)                      corp_gen_deps_y_avg,
                ifnull(a.inc_corp_gen_deps_q_avg, 0)                  inc_corp_gen_deps_q_avg,
                ifnull(a.inc_corp_gen_deps_y_avg, 0)                  inc_corp_gen_deps_y_avg,
                ifnull(b.per_gen_deps_q_avg, 0)                       per_gen_deps_q_avg,
                ifnull(b.per_gen_deps_y_avg, 0)                       per_gen_deps_y_avg,
                ifnull(b.per_margin_deps_q_avg, 0)                    per_margin_deps_q_avg,
                ifnull(b.per_margin_deps_y_avg, 0)                    per_margin_deps_y_avg,
                ifnull(c.hxb_no_loan_q_ir, 0)                         hxb_no_loan_q_ir,
                ifnull(c.hxb_per_no_loan_q_ir, 0)                     hxb_per_no_loan_q_ir,
                ifnull(c.per_mm_pas_q_avg, 0)                         per_mm_pas_q_avg,
                ifnull(c.per_mm_pas_y_avg, 0)                         per_mm_pas_y_avg,
                ifnull(c.per_mm_pas_q_ir, 0)                          per_mm_pas_q_ir,
                ifnull(c.per_pure_deps_q_avg, 0)                      per_pure_deps_q_avg,
                ifnull(c.per_no_loan_q_ir, 0)                         per_no_loan_q_ir,
                ifnull(c.corp_no_loan_deps_q_avg, 0)                  corp_no_loan_deps_q_avg,
                ifnull(c.corp_no_loan_q_ir, 0)                        corp_no_loan_q_ir,
                ifnull(c.gen_pure_deps_q_avg, 0)                      gen_pure_deps_q_avg,
                ifnull(c.gen_pure_q_ir, 0)                            gen_pure_q_ir,
                ifnull(a.less_fifty_million_cust, 0)                  less_fifty_million_cust,
                ifnull(a.fifty_million_cust, 0)                       fifty_million_cust,
                ifnull(a.one_hundred_million_cust, 0)                 one_hundred_million_cust
         from tmp_mgr_corp_gen_deps_avg a
                  full outer join tmp_mgr_per_gen_deps_avg b
                                  on a.absorb_code = b.absorb_code
                  full outer join tmp_mgr_pure_deps_avg c
                                  on coalesce(a.absorb_code, b.absorb_code) = c.absorb_code
     )
insert
overwrite
table
hxb_dh_data_dws.dws_staff_dep_loan_sum
select a.absorb_code,
       '',
       ifnull(a.corp_gen_deps_q_avg, 0)      corp_gen_deps_q_avg,      --对公存款季日均
       ifnull(a.corp_gen_deps_y_avg, 0)      corp_gen_deps_y_avg,      --对公存款年日均
       ifnull(a.per_gen_deps_q_avg, 0)       per_gen_deps_q_avg,       --储蓄存款季日均
       ifnull(a.per_gen_deps_y_avg, 0)       per_gen_deps_y_avg,       --储蓄存款年日均
       ifnull(a.per_margin_deps_q_avg, 0)    per_margin_deps_q_avg,    --储蓄保证金存款季日均
       ifnull(a.per_margin_deps_y_avg, 0)    per_margin_deps_y_avg,    --储蓄保证金存款年日均
       ifnull(b.per_hui_surplus_q_avg, 0)    per_hui_surplus_q_avg,    --个人慧盈季日均
       ifnull(b.per_hui_surplus_y_avg, 0)    per_hui_surplus_y_avg,    --个人慧盈年日均
       ifnull(a.per_mm_pas_q_avg, 0)         per_mm_pas_q_avg,         --个人理财在途季日均
       ifnull(a.per_mm_pas_y_avg, 0)         per_mm_pas_y_avg,         --个人理财在途年日均
       ifnull(a.per_mm_pas_q_ir, 0)          per_mm_pas_q_ir,          --理财在途付息率(季度)
       ifnull(a.per_pure_deps_q_avg, 0)      per_pure_deps_q_avg,      --储蓄无贷户纯存款季度日均
       ifnull(a.per_no_loan_q_ir, 0)         per_no_loan_q_ir,         --储蓄无贷户纯存款付息率(季度)
       ifnull(a.hxb_per_no_loan_q_ir, 0)     hxb_per_no_loan_q_ir,     --全行储蓄无贷户纯存款付息率(季度)
       ifnull(a.corp_no_loan_deps_q_avg, 0)  corp_no_loan_deps_q_avg,  --对公无贷户纯存款季度日均
       ifnull(a.corp_no_loan_q_ir, 0)        corp_no_loan_q_ir,        --对公无贷户纯存款付息率(季度)
       ifnull(a.gen_pure_deps_q_avg, 0)      gen_pure_deps_q_avg,      --一般性无贷户纯存款季度日均
       ifnull(a.gen_pure_q_ir, 0)            gen_pure_q_ir,            --一般性无贷户纯存款付息率(季度)
       ifnull(a.hxb_no_loan_q_ir, 0)         hxb_no_loan_q_ir,         --全行一般性无贷户纯存款付息率(季度)
       (ifnull(a.per_gen_deps_q_avg, 0) - ifnull(a.per_margin_deps_q_avg, 0)) * 3
           + ifnull(a.per_margin_deps_q_avg, 0)
           + ifnull(a.per_mm_pas_q_avg, 0) * 3
           + ifnull(b.per_hui_surplus_q_avg, 0)
           + ifnull(a.corp_gen_deps_q_avg, 0) compr_deps_avg_q, -- 综合存款季度日均,储蓄非保证类存款*3+储蓄保证类*1+理财在途*3+慧盈理财*1+对公所有*1
       (ifnull(a.per_gen_deps_y_avg, 0) - ifnull(a.per_margin_deps_y_avg, 0)) * 3
           + ifnull(a.per_margin_deps_y_avg, 0)
           + ifnull(a.per_mm_pas_y_avg, 0) * 3
           + ifnull(b.per_hui_surplus_y_avg, 0)
           + ifnull(a.corp_gen_deps_y_avg, 0) compr_deps_avg_y, -- 综合存款年度日均,储蓄非保证类存款*3+储蓄保证类*1+理财在途*3+慧盈理财*1+对公所有*1
       ifnull(a.inc_corp_gen_deps_q_avg, 0)  inc_corp_gen_deps_q_avg,  --净增全口径对公存款日均季度
       ifnull(a.inc_corp_gen_deps_y_avg, 0)  inc_corp_gen_deps_y_avg,  --净增全口径对公存款日均年度
       ifnull(c.inc_corp_loan_credit_bal, 0) inc_corp_loan_credit_bal, --净增对公贷款
       ifnull(c.corp_loan_credit_bal, 0)     corp_loan_credit_bal,     --对公贷款用信余额
       0,--     inc_inclus_dbl_inc_q     '新增普惠条线两增贷款(季)',
       0,--     inc_inclus_dbl_inc_y     '新增普惠条线两增贷款(年）',
       0,--     inc_inclus_loan_q        '新增普惠条线贷款(季)',
       0,--     inc_inclus_loan_y        '新增普惠条线贷款(年）',
       0,--     inclus_dbl_inc           '普惠条线两增贷款',           -- 余额
       0,--     inclus_loan_bal          '普惠贷款规模',-- 余额
       0,--     inclus_loan_cust         '普惠贷款客户数',
       0,--     inc_dbl_inc_cust_q       '净增普惠条线两增客户数(季）',
       0,--     inc_dbl_inc_cust_y       '净增普惠条线两增客户数（年）',
       0,--     inc_inclus_loan_cust_q   '净增普惠条线贷款客户数（季）',
       0,--     inc_inclus_loan_cust_y   '净增普惠条线贷款客户数（年）',
       ifnull(a.less_fifty_million_cust, 0)  less_fifty_million_cust,  --小于5000万对公有效户数
       ifnull(a.fifty_million_cust, 0)       fifty_million_cust,       --5000万对公有效户数
       ifnull(a.one_hundred_million_cust, 0) one_hundred_million_cust, --1亿对公有效户数
       0,-- per_loan_delivery_q      decimal(30, 6) comment '个贷当季投放',
       0,-- per_loan_delivery_y      decimal(30, 6) comment '个贷累计投放',
       ifnull(inc_fin_avg_q, 0)              inc_fin_avg_q,            -- 净增理财日均
       to_date('${date}')
from tmp_mgr_deps a
         full outer join tmp_mgr_fin b
                         on a.absorb_code = b.absorb_code
         full outer join tmp_corp_loan_borm c
                         on coalesce(a.absorb_code, b.absorb_code) = c.absorb_code

