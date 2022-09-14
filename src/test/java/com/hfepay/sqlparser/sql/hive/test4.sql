select a.ACCT_NO,
       a.BRANCH_NO,
       a.customer_no,
       a.acct_type,
       c.int_cat,
       c.descript,
       case
           when c.descript like '%年年乐%' then '年年乐'
           when c.acct_desc = 'S' and a.acct_type != '4999' then '活期存款'
           when c.acct_desc = 'T' and a.acct_type != '4999' then '定期存款'
           when a.acct_type = '4999' then '大额存单'
           else c.acct_desc
           end                                                                                                   as deposit_type,
       c.acct_desc,
       c.customer_type,
       a.cr_store_rate,
       wjj.baserate,
       case
           when cast(IFNULL(a.cr_store_rate, 0.00) as decimal(18, 2)) > IFNULL(wjj.baserate, 0.00) then 'N'
           else 'Y'
           end                                                                                                      is_basic,
       a.CURRENCY,
       d.RATE_MID,
       a.CURR_BAL,
       cast(cast(IFNULL(a.curr_bal, 0.00) as decimal(18, 2)) * IFNULL(d.rate_mid, 0.00) /
            100 as decimal(18, 2))                                                                               as curr_bal_r, -- 余额折RMB
       a.ETL_DATE
from hxb_dh_data.bcs_invm a
         left join hxb_dh_data.bcs_depp c on concat(a.ACCT_TYPE, a.INT_CAT) = concat(c.TYPE_TD, c.INT_CAT) and
                                             c.BRANCH_NO_PARTITION = '1100' and c.ETL_DATE = a.etl_date
         left join hxb_dh_data.bcs_curm d
                   on a.CURRENCY = d.KEY_1 and d.BRANCH_NO_PARTITION = '1100' and d.ETL_DATE = a.etl_date
         left join hxb_dh_data.v_wh_ck_baserate wjj
                   on wjj.type_td = a.acct_type and wjj.int_cat = a.int_cat and a.ETL_DATE = wjj.etl_date
where c.customer_type = 'P'
  and concat(c.TYPE_TD, c.int_cat) not in ('43010010', '43150010')
  and a.etl_date = '${date}'