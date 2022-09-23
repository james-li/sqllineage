with tmp_fee_details as (
    SELECT branch_no,
           branch_name,
           deposit_type,
           pro_type,
           rate_des,
           rate_std,
           rate_std_i,
           head_rate,
           head_rate_i,
           last_year_avg_bal_r_1,
           curr_year_avg_bal_r_1,
           avg_bal_r_1,
           avg_bal_r_i_1,
           last_year_avg_bal_r_2,
           curr_year_avg_bal_r_2,
           avg_bal_r_2,
           avg_bal_r_i_2,
           avg_bal_r_sum_2,
           avg_bal_r_i_sum_2,
           last_year_avg_bal_r_3,
           curr_year_avg_bal_r_3,
           avg_bal_r_3,
           avg_bal_r_i_3,
           avg_bal_r_sum_3,
           avg_bal_r_i_sum_3,
           avg_bal_r_flod,
           avg_bal_r_i_flod,
           fee,
           fee_i_1,
           fee_i_11,
           fee_i_12,
           etl_date
    FROM hxb_dh_data_dws.dws_deposit_branch_sales_fee
    where etl_date = '${date}'
),
     tmp_last_fee_details as (
         SELECT branch_no,
                branch_name,
                deposit_type,
                pro_type,
                rate_des,
                rate_std,
                rate_std_i,
                head_rate,
                head_rate_i,
                last_year_avg_bal_r_1,
                curr_year_avg_bal_r_1,
                avg_bal_r_1,
                avg_bal_r_i_1,
                last_year_avg_bal_r_2,
                curr_year_avg_bal_r_2,
                avg_bal_r_2,
                avg_bal_r_i_2,
                avg_bal_r_sum_2,
                avg_bal_r_i_sum_2,
                last_year_avg_bal_r_3,
                curr_year_avg_bal_r_3,
                avg_bal_r_3,
                avg_bal_r_i_3,
                avg_bal_r_sum_3,
                avg_bal_r_i_sum_3,
                avg_bal_r_flod,
                avg_bal_r_i_flod,
                fee,
                fee_i_1,
                fee_i_11,
                fee_i_12,
                etl_date
         FROM hxb_dh_data_dws.dws_deposit_branch_sales_fee
         where etl_date =
               last_day(concat(year('${date}'), '-', substr(concat('0', floor((month('${date}') + 2) / 3 - 1) * 3), -2),
                                                '-01')) -- 上一季度最后一天
     ),
     tmp_fee_diff_details as (
         SELECT ifnull(a.branch_no, b.branch_no)              branch_no,
                ifnull(a.branch_name, b.branch_name)          branch_name,
                ifnull(a.deposit_type, b.deposit_type)        deposit_type,
                ifnull(a.pro_type, b.pro_type)                pro_type,
                ifnull(a.rate_des, b.rate_des)                rate_des,
                ifnull(a.rate_std, b.rate_std)                rate_std,
                ifnull(a.rate_std_i, b.rate_std_i)            rate_std_i,
                ifnull(a.head_rate, 0)                        head_rate,
                ifnull(a.head_rate_i, 0)                      head_rate_i,
                ifnull(a.last_year_avg_bal_r_1, 0)            last_year_avg_bal_r_1,
                ifnull(a.curr_year_avg_bal_r_1, 0)            curr_year_avg_bal_r_1,
                ifnull(a.avg_bal_r_1, 0)                      avg_bal_r_1,
                ifnull(a.avg_bal_r_i_1, 0)                    avg_bal_r_i_1,
                ifnull(a.last_year_avg_bal_r_2, 0)            last_year_avg_bal_r_2,
                ifnull(a.curr_year_avg_bal_r_2, 0)            curr_year_avg_bal_r_2,
                ifnull(a.avg_bal_r_2, 0)                      avg_bal_r_2,
                ifnull(a.avg_bal_r_i_2, 0)                    avg_bal_r_i_2,
                ifnull(a.avg_bal_r_sum_2, 0)                  avg_bal_r_sum_2,
                ifnull(a.avg_bal_r_i_sum_2, 0)                avg_bal_r_i_sum_2,
                ifnull(a.last_year_avg_bal_r_3, 0)            last_year_avg_bal_r_3,
                ifnull(a.curr_year_avg_bal_r_3, 0)            curr_year_avg_bal_r_3,
                ifnull(a.avg_bal_r_3, 0)                      avg_bal_r_3,
                ifnull(a.avg_bal_r_i_3, 0)                    avg_bal_r_i_3,
                ifnull(a.avg_bal_r_sum_3, 0)                  avg_bal_r_sum_3,
                ifnull(a.avg_bal_r_i_sum_3, 0)                avg_bal_r_i_sum_3,
                ifnull(a.avg_bal_r_flod, 0)                   avg_bal_r_flod,
                ifnull(a.avg_bal_r_i_flod, 0)                 avg_bal_r_i_flod,
                ifnull(a.fee, 0)                              fee,
                ifnull(a.fee_i_1, 0)                          fee_i_1,
                ifnull(a.fee_i_11, 0)                         fee_i_11,
                ifnull(a.fee_i_12, 0)                         fee_i_12,
                ifnull(b.fee, 0)                              last_fee,
                ifnull(b.fee_i_1, 0)                          last_fee_i_1,
                ifnull(b.fee_i_11, 0)                         last_fee_i_11,
                ifnull(b.fee_i_12, 0)                         last_fee_i_12,
                ifnull(a.fee, 0) - ifnull(b.fee, 0)           end_fee,
                ifnull(a.fee_i_1, 0) - ifnull(b.fee_i_1, 0)   end_fee_i_1,
                ifnull(a.fee_i_11, 0) - ifnull(b.fee_i_11, 0) end_fee_i_11,
                ifnull(a.fee_i_12, 0) - ifnull(b.fee_i_12, 0) end_fee_i_12
         from tmp_fee_details a
                  full outer join tmp_last_fee_details b
                                  on a.branch_no = b.branch_no and a.deposit_type = b.deposit_type
                                      and a.pro_type = b.pro_type and a.rate_des = b.rate_des
     ),
     fee_sum as (
         select a.branch_no,                                                                                 --  `机构号`,
                a.branch_name,                                                                               --  `机构名称`,
                a.deposit_type,                                                                            --  `存款类型`,
                c.stock_rate                                                                head_rate,     -- `与总行日均存量折比例`,
                c.increment_rate                                                            head_rate_i,   -- `与总行日均增量折比例`,
                sum(fee)                                                                    fee,           --  `存量费用`,
                sum(fee_i_1)                                                                fee_i_1,       --  `增量费用调节系数1`,
                sum(fee_i_11)                                                               fee_i_11,      --  `增量费用调节系数11`,
                sum(fee_i_12)                                                               fee_i_12,      --  `增量费用调节系数12`,
                sum(fee) + sum(fee_i_1) + sum(fee_i_11) + sum(fee_i_12)                     fee_sum,-- `总费用`,
                sum(last_fee)                                                               last_fee,      --  `存量费用`,
                sum(last_fee_i_1)                                                           last_fee_i_1,  --  `增量费用调节系数1`,
                sum(last_fee_i_11)                                                          last_fee_i_11, --  `增量费用调节系数11`,
                sum(last_fee_i_12)                                                          last_fee_i_12, --  `增量费用调节系数12`,
                sum(last_fee) + sum(last_fee_i_1) + sum(last_fee_i_11) + sum(last_fee_i_12) last_fee_sum,-- `总费用`,
                sum(end_fee)                                                                end_fee,       --  `存量费用`,
                sum(end_fee_i_1)                                                            end_fee_i_1,   --  `增量费用调节系数1`,
                sum(end_fee_i_11)                                                           end_fee_i_11,  --  `增量费用调节系数11`,
                sum(end_fee_i_12)                                                           end_fee_i_12,  --  `增量费用调节系数12`,
                sum(end_fee) + sum(end_fee_i_1) + sum(end_fee_i_11) + sum(end_fee_i_12)     end_fee_sum    -- `总费用`,
         from tmp_fee_diff_details a
                  left join hxb_dh_data_dws.dws_basic_deposit_ratio_to_head c
                            on c.etl_date = '${date}' and a.deposit_type = c.deposit_type

         group by branch_no, branch_name, a.deposit_type, c.stock_rate, c.increment_rate
         order by branch_no, deposit_type
     )
insert
overwrite
table
hxb_dh_data_ads.ads_deposit_branch_sales_fee_wide_new
select a.branch_no                           `机构号`,
       a.branch_name                         `机构名称`,
       a.deposit_type                        `存款类型`,
       a.end_fee                             `最终存量费用`,
       a.end_fee_i_1                         `最终增量费用调节系数1`,
       a.end_fee_i_11                        `最终增量费用调节系数11`,
       a.end_fee_i_12                        `最终增量费用调节系数12`,
       a.end_fee_sum                         `最终总费用`,
       a.fee                                 `减之前存量费用`,
       a.fee_i_1                             `减之前增量费用调节系数1`,
       a.fee_i_11                            `减之前增量费用调节系数11`,
       a.fee_i_12                            `减之前增量费用调节系数12`,
       a.fee_sum                             `减之前总费用`,
       a.last_fee                            `本季之前存量费用`,
       a.last_fee_i_1                        `本季之前增量费用调节系数1`,
       a.last_fee_i_11                       `本季之前增量费用调节系数11`,
       a.last_fee_i_12                       `本季之前增量费用调节系数12`,
       a.last_fee_sum                        `本季之前总费用`,
       a.head_rate                           `与总行日均存量折比例`,
       a.head_rate_i                         `与总行日均增量折比例`,
       ifnull(hq01.last_year_avg_bal_r_1, 0) `基础存款去年日均`,
       -- ifnull(hq01.curr_quarter_avg_bal_r_1, 0) `基础存款当季日均`,
       ifnull(hq01.curr_year_avg_bal_r_1, 0) `基础存款当年年日均`,
       ifnull(hq01.avg_bal_r_1, 0)           `基础存款日均存量`,
       ifnull(hq01.avg_bal_r_i_1, 0)         `基础存款日均增量`,
       ifnull(hq01.last_year_avg_bal_r_2, 0) `基础存款活期去年日均`,
       -- ifnull(hq01.curr_quarter_avg_bal_r_2, 0) `基础存款活期当季日均`,
       ifnull(hq01.curr_year_avg_bal_r_2, 0) `基础存款活期当年年日均`,
       ifnull(hq01.avg_bal_r_2, 0)           `基础存款活期日均存量`,
       ifnull(hq01.avg_bal_r_i_2, 0)         `基础存款活期日均增量`,

       ifnull(hq01.rate_std, 0)              `活期不超过挂牌利率存量费率标准`,
       ifnull(hq01.rate_std_i, 0)            `活期不超过挂牌利率增量费率标准`,
       ifnull(hq01.last_year_avg_bal_r_3, 0) `基础存款活期不超过挂牌利率去年日均`,
       -- ifnull(hq01.curr_quarter_avg_bal_r_3, 0) `基础存款活期不超过挂牌利率当季日均`,
       ifnull(hq01.curr_year_avg_bal_r_3, 0) `基础存款活期不超过挂牌利率当年年日均`,
       ifnull(hq01.avg_bal_r_3, 0)           `基础存款活期不超过挂牌利率存量日均`,
       ifnull(hq01.avg_bal_r_i_3, 0)         `基础存款活期不超过挂牌利率增量日均`,
       ifnull(hq01.avg_bal_r_flod, 0)        `活期不超过挂牌利率折后日均存量`,
       ifnull(hq01.avg_bal_r_i_flod, 0)      `活期不超过挂牌利率折后日均增量`,

       ifnull(hq01.end_fee, 0)               `活期不超过挂牌利率最终存量费用`,
       ifnull(hq01.end_fee_i_1, 0)           `活期不超过挂牌利率最终增量费用调节系数1`,
       ifnull(hq01.end_fee_i_11, 0)          `活期不超过挂牌利率最终增量费用调节系数11`,
       ifnull(hq01.end_fee_i_12, 0)          `活期不超过挂牌利率最终增量费用调节系数12`,
       ifnull(hq01.fee, 0)                   `活期不超过挂牌利率减之前存量费用`,
       ifnull(hq01.fee_i_1, 0)               `活期不超过挂牌利率减之前增量费用调节系数1`,
       ifnull(hq01.fee_i_11, 0)              `活期不超过挂牌利率减之前增量费用调节系数11`,
       ifnull(hq01.fee_i_12, 0)              `活期不超过挂牌利率减之前增量费用调节系数12`,
       ifnull(hq01.last_fee, 0)              `活期不超过挂牌利率本季之前存量费用`,
       ifnull(hq01.last_fee_i_1, 0)          `活期不超过挂牌利率本季之前增量费用调节系数1`,
       ifnull(hq01.last_fee_i_11, 0)         `活期不超过挂牌利率本季之前增量费用调节系数11`,
       ifnull(hq01.last_fee_i_12, 0)         `活期不超过挂牌利率本季之前增量费用调节系数12`,

       ifnull(hq02.rate_std, 0)              `活期不超过央行基准利率加点20bp存量费率标准`,
       ifnull(hq02.rate_std_i, 0)            `活期不超过央行基准利率加点20bp增量费率标准`,
       ifnull(hq02.last_year_avg_bal_r_3, 0) `基础存款活期不超过央行基准利率加点20bp去年日均`,
       -- ifnull(hq02.curr_quarter_avg_bal_r_3, 0) `基础存款活期不超过央行基准利率加点20bp当季日均`,
       ifnull(hq02.curr_year_avg_bal_r_3, 0) `基础存款活期不超过央行基准利率加点20bp当年年日均`,
       ifnull(hq02.avg_bal_r_3, 0)           `基础存款活期不超过央行基准利率加点20bp存量日均`,
       ifnull(hq02.avg_bal_r_i_3, 0)         `基础存款活期不超过央行基准利率加点20bp增量日均`,
       ifnull(hq02.avg_bal_r_flod, 0)        `基础存款活期不超过央行基准利率加点20bp折后日均存量`,
       ifnull(hq02.avg_bal_r_i_flod, 0)      `基础存款活期不超过央行基准利率加点20bp折后日均增量`,

       ifnull(hq02.end_fee, 0)               `活期不超过央行基准利率加点20bp最终存量费用`,
       ifnull(hq02.end_fee_i_1, 0)           `活期不超过央行基准利率加点20bp最终增量费用调节系数1`,
       ifnull(hq02.end_fee_i_11, 0)          `活期不超过央行基准利率加点20bp最终增量费用调节系数11`,
       ifnull(hq02.end_fee_i_12, 0)          `活期不超过央行基准利率加点20bp最终增量费用调节系数12`,
       ifnull(hq02.fee, 0)                   `活期不超过央行基准利率加点20bp减之前存量费用`,
       ifnull(hq02.fee_i_1, 0)               `活期不超过央行基准利率加点20bp减之前增量费用调节系数1`,
       ifnull(hq02.fee_i_11, 0)              `活期不超过央行基准利率加点20bp减之前增量费用调节系数11`,
       ifnull(hq02.fee_i_12, 0)              `活期不超过央行基准利率加点20bp减之前增量费用调节系数12`,
       ifnull(hq02.last_fee, 0)              `活期不超过央行基准利率加点20bp本季之前存量费用`,
       ifnull(hq02.last_fee_i_1, 0)          `活期不超过央行基准利率加点20bp本季之前增量费用调节系数1`,
       ifnull(hq02.last_fee_i_11, 0)         `活期不超过央行基准利率加点20bp本季之前增量费用调节系数11`,
       ifnull(hq02.last_fee_i_12, 0)         `活期不超过央行基准利率加点20bp本季之前增量费用调节系数12`,

       ifnull(dq01.last_year_avg_bal_r_2, 0) `基础存款(定期/协定)去年日均`,
       -- ifnull(dq01.curr_quarter_avg_bal_r_2, 0) `基础存款(定期/协定)当季日均`,
       ifnull(dq01.curr_year_avg_bal_r_2, 0) `基础存款(定期/协定)当年年日均`,
       ifnull(dq01.avg_bal_r_2, 0)           `基础存款(定期/协定)存量日均`,
       ifnull(dq01.avg_bal_r_i_2, 0)         `基础存款(定期/协定)增量日均`,

       ifnull(dq01.rate_std, 0)              `(定期/协定)不超过挂牌利率存量费率标准`,
       ifnull(dq01.rate_std_i, 0)            `(定期/协定)不超过挂牌利率增量费率标准`,
       ifnull(dq01.last_year_avg_bal_r_3, 0) `基础存款(定期/协定)不超过挂牌利率去年日均`,
       -- ifnull(dq01.curr_quarter_avg_bal_r_3, 0) `基础存款(定期/协定)不超过挂牌利率当季日均`,
       ifnull(dq01.curr_year_avg_bal_r_3, 0) `基础存款(定期/协定)不超过挂牌利率当年年日均`,
       ifnull(dq01.avg_bal_r_3, 0)           `基础存款(定期/协定)不超过挂牌利率存量日均`,
       ifnull(dq01.avg_bal_r_i_3, 0)         `基础存款(定期/协定)不超过挂牌利率增量日均`,
       ifnull(dq01.avg_bal_r_flod, 0)        `(定期/协定)不超过挂牌利率折后日均存量`,
       ifnull(dq01.avg_bal_r_i_flod, 0)      `(定期/协定)不超过挂牌利率折后日均增量`,
       ifnull(dq01.end_fee, 0)               `(定期/协定)不超过挂牌利率最终存量费用`,
       ifnull(dq01.end_fee_i_1, 0)           `(定期/协定)不超过挂牌利率最终增量费用调节系数1`,
       ifnull(dq01.end_fee_i_11, 0)          `(定期/协定)不超过挂牌利率最终增量费用调节系数11`,
       ifnull(dq01.end_fee_i_12, 0)          `(定期/协定)不超过挂牌利率最终增量费用调节系数12`,
       ifnull(dq01.fee, 0)                   `(定期/协定)不超过挂牌利率减之前存量费用`,
       ifnull(dq01.fee_i_1, 0)               `(定期/协定)不超过挂牌利率减之前增量费用调节系数1`,
       ifnull(dq01.fee_i_11, 0)              `(定期/协定)不超过挂牌利率减之前增量费用调节系数11`,
       ifnull(dq01.fee_i_12, 0)              `(定期/协定)不超过挂牌利率减之前增量费用调节系数12`,
       ifnull(dq01.last_fee, 0)              `(定期/协定)不超过挂牌利率本季之前存量费用`,
       ifnull(dq01.last_fee_i_1, 0)          `(定期/协定)不超过挂牌利率本季之前增量费用调节系数1`,
       ifnull(dq01.last_fee_i_11, 0)         `(定期/协定)不超过挂牌利率本季之前增量费用调节系数11`,
       ifnull(dq01.last_fee_i_12, 0)         `(定期/协定)不超过挂牌利率本季之前增量费用调节系数12`,

       ifnull(dq02.rate_std, 0)              `(定期/协定)不超过央行基准利率加点20bp存量费率标准`,
       ifnull(dq02.rate_std_i, 0)            `(定期/协定)不超过央行基准利率加点20bp增量费率标准`,
       ifnull(dq02.last_year_avg_bal_r_3, 0) `基础存款(定期/协定)不超过央行基准利率加点20bp去年日均`,
       -- ifnull(dq02.curr_quarter_avg_bal_r_3, 0) `基础存款(定期/协定)不超过央行基准利率加点20bp当季日均`,
       ifnull(dq02.curr_year_avg_bal_r_3, 0) `基础存款(定期/协定)不超过央行基准利率加点20bp当年年日均`,
       ifnull(dq02.avg_bal_r_3, 0)           `基础存款(定期/协定)不超过央行基准利率加点20bp存量日均`,
       ifnull(dq02.avg_bal_r_i_3, 0)         `基础存款(定期/协定)不超过央行基准利率加点20bp增量日均`,
       ifnull(dq02.avg_bal_r_flod, 0)        `(定期/协定)不超过央行基准利率加点20bp折后日均存量`,
       ifnull(dq02.avg_bal_r_i_flod, 0)      `(定期/协定)不超过央行基准利率加点20bp折后日均增量`,

       ifnull(dq02.end_fee, 0)               `(定期/协定)不超过央行基准利率加点20bp率最终存量费用`,
       ifnull(dq02.end_fee_i_1, 0)           `(定期/协定)不超过央行基准利率加点20bp率最终增量费用调节系数1`,
       ifnull(dq02.end_fee_i_11, 0)          `(定期/协定)不超过央行基准利率加点20bp率最终增量费用调节系数11`,
       ifnull(dq02.end_fee_i_12, 0)          `(定期/协定)不超过央行基准利率加点20bp率最终增量费用调节系数12`,
       ifnull(dq02.fee, 0)                   `(定期/协定)不超过央行基准利率加点20bp率减之前存量费用`,
       ifnull(dq02.fee_i_1, 0)               `(定期/协定)不超过央行基准利率加点20bp率减之前增量费用调节系数1`,
       ifnull(dq02.fee_i_11, 0)              `(定期/协定)不超过央行基准利率加点20bp率减之前增量费用调节系数11`,
       ifnull(dq02.fee_i_12, 0)              `(定期/协定)不超过央行基准利率加点20bp率减之前增量费用调节系数12`,
       ifnull(dq02.last_fee, 0)              `(定期/协定)不超过央行基准利率加点20bp率本季之前存量费用`,
       ifnull(dq02.last_fee_i_1, 0)          `(定期/协定)不超过央行基准利率加点20bp率本季之前增量费用调节系数1`,
       ifnull(dq02.last_fee_i_11, 0)         `(定期/协定)不超过央行基准利率加点20bp率本季之前增量费用调节系数11`,
       ifnull(dq02.last_fee_i_12, 0)         `(定期/协定)不超过央行基准利率加点20bp率本季之前增量费用调节系数12`,

       ifnull(dq03.rate_std, 0)              `定期两年及以内的加点不超过80个bp的个人大额存单存量费率标准`,
       ifnull(dq03.rate_std_i, 0)            `定期两年及以内的加点不超过80个bp的个人大额存单增量费率标准`,
       ifnull(dq03.last_year_avg_bal_r_3, 0) `基础存款定期两年及以内的加点不超过80个bp的个人大额存单去年日均`,
       -- ifnull(dq03.curr_quarter_avg_bal_r_3, 0) `基础存款定期两年及以内的加点不超过80个bp的个人大额存单当季日均`,
       ifnull(dq03.curr_year_avg_bal_r_3, 0) `基础存款定期两年及以内的加点不超过80个bp的个人大额存单当年年日均`,
       ifnull(dq03.avg_bal_r_3, 0)           `基础存款定期两年及以内的加点不超过80个bp的个人大额存单存量日均`,
       ifnull(dq03.avg_bal_r_i_3, 0)         `基础存款定期两年及以内的加点不超过80个bp的个人大额存单增量日均`,
       ifnull(dq03.avg_bal_r_flod, 0)        `定期两年及以内的加点不超过80个bp的个人大额存单折后日均存量`,
       ifnull(dq03.avg_bal_r_i_flod, 0)      `定期两年及以内的加点不超过80个bp的个人大额存单折后日均增量`,

       ifnull(dq03.end_fee, 0)               `定期两年及以内的加点不超过80个bp的个人大额存单最终存量费用`,
       ifnull(dq03.end_fee_i_1, 0)           `定期两年及以内的加点不超过80个bp的个人大额存单最终增量费用调节系数1`,
       ifnull(dq03.end_fee_i_11, 0)          `定期两年及以内的加点不超过80个bp的个人大额存单最终增量费用调节系数11`,
       ifnull(dq03.end_fee_i_12, 0)          `定期两年及以内的加点不超过80个bp的个人大额存单最终增量费用调节系数12`,
       ifnull(dq03.fee, 0)                   `定期两年及以内的加点不超过80个bp的个人大额存单减之前存量费用`,
       ifnull(dq03.fee_i_1, 0)               `定期两年及以内的加点不超过80个bp的个人大额存单减之前增量费用调节系数1`,
       ifnull(dq03.fee_i_11, 0)              `定期两年及以内的加点不超过80个bp的个人大额存单减之前增量费用调节系数11`,
       ifnull(dq03.fee_i_12, 0)              `定期两年及以内的加点不超过80个bp的个人大额存单减之前增量费用调节系数12`,
       ifnull(dq03.last_fee, 0)              `定期两年及以内的加点不超过80个bp的个人大额存单本季之前存量费用`,
       ifnull(dq03.last_fee_i_1, 0)          `定期两年及以内的加点不超过80个bp的个人大额存单本季之前增量费用调节系数1`,
       ifnull(dq03.last_fee_i_11, 0)         `定期两年及以内的加点不超过80个bp的个人大额存单本季之前增量费用调节系数11`,
       ifnull(dq03.last_fee_i_12, 0)         `定期两年及以内的加点不超过80个bp的个人大额存单本季之前增量费用调节系数12`,

       ifnull(wb01.last_year_avg_bal_r_2, 0) `基础存款(外币一年期及以内/外币活期)去年日均`,
       -- ifnull(wb01.curr_quarter_avg_bal_r_2, 0) `基础存款(外币一年期及以内/外币活期)当季日均`,
       ifnull(wb01.curr_year_avg_bal_r_2, 0) `基础存款(外币一年期及以内/外币活期)当年年日均`,
       ifnull(wb01.avg_bal_r_2, 0)           `基础存款(外币一年期及以内/外币活期)存量日均`,
       ifnull(wb01.avg_bal_r_i_2, 0)         `基础存款(外币一年期及以内/外币活期)增量日均`,

       ifnull(wb01.rate_std, 0)              `基础存款(外币一年期及以内/外币活期)存量费率标准`,
       ifnull(wb01.rate_std_i, 0)            `基础存款(外币一年期及以内/外币活期)增量费率标准`,
       ifnull(wb01.last_year_avg_bal_r_3, 0) `基础存款(外币一年期及以内/外币活期)去年日均`,
       -- ifnull(wb01.curr_quarter_avg_bal_r_3, 0) `基础存款(外币一年期及以内/外币活期)当季日均`,
       ifnull(wb01.curr_year_avg_bal_r_3, 0) `基础存款(外币一年期及以内/外币活期)当年年日均`,
       ifnull(wb01.avg_bal_r_3, 0)           `基础存款(外币一年期及以内/外币活期)存量日均`,
       ifnull(wb01.avg_bal_r_i_3, 0)         `基础存款(外币一年期及以内/外币活期)增量日均`,
       ifnull(wb01.avg_bal_r_flod, 0)        `(外币一年期及以内/外币活期)折后日均存量`,
       ifnull(wb01.avg_bal_r_i_flod, 0)      `(外币一年期及以内/外币活期)折后日均增量`,

       ifnull(wb01.end_fee, 0)               `(外币一年期及以内/外币活期)最终存量费用`,
       ifnull(wb01.end_fee_i_1, 0)           `(外币一年期及以内/外币活期)最终增量费用调节系数1`,
       ifnull(wb01.end_fee_i_11, 0)          `(外币一年期及以内/外币活期)最终增量费用调节系数11`,
       ifnull(wb01.end_fee_i_12, 0)          `(外币一年期及以内/外币活期)最终增量费用调节系数12`,
       ifnull(wb01.fee, 0)                   `(外币一年期及以内/外币活期)减之前存量费用`,
       ifnull(wb01.fee_i_1, 0)               `(外币一年期及以内/外币活期)减之前增量费用调节系数1`,
       ifnull(wb01.fee_i_11, 0)              `(外币一年期及以内/外币活期)减之前增量费用调节系数11`,
       ifnull(wb01.fee_i_12, 0)              `(外币一年期及以内/外币活期)减之前增量费用调节系数12`,
       ifnull(wb01.last_fee, 0)              `(外币一年期及以内/外币活期)本季之前存量费用`,
       ifnull(wb01.last_fee_i_1, 0)          `(外币一年期及以内/外币活期)本季之前增量费用调节系数1`,
       ifnull(wb01.last_fee_i_11, 0)         `(外币一年期及以内/外币活期)本季之前增量费用调节系数11`,
       ifnull(wb01.last_fee_i_12, 0)         `(外币一年期及以内/外币活期)本季之前增量费用调节系数12`,

       to_date('${date}')                    `日期`
from fee_sum a
         left join tmp_fee_diff_details hq01
                   on a.branch_no = hq01.branch_no and
                      a.deposit_type = hq01.deposit_type and
                      hq01.pro_type = '活期' and
                      hq01.rate_des = '不超过挂牌利率(含)'
         left join tmp_fee_diff_details hq02
                   on a.branch_no = hq02.branch_no and
                      a.deposit_type = hq02.deposit_type and
                      hq02.pro_type = '活期' and
                      hq02.rate_des = '挂牌利率-不超过央行基准利率加点20bp(含)'
         left join tmp_fee_diff_details dq01
                   on a.branch_no = dq01.branch_no and
                      a.deposit_type = dq01.deposit_type and
                      (dq01.pro_type = '定期' or dq01.pro_type = '协定') and -- 定期/协定
                      dq01.rate_des = '不超过挂牌利率(含)'
         left join tmp_fee_diff_details dq02
                   on a.branch_no = dq02.branch_no and
                      a.deposit_type = dq02.deposit_type and
                      (dq02.pro_type = '定期' or dq02.pro_type = '协定') and -- 定期/协定
                      dq02.rate_des = '挂牌利率-不超过央行基准利率加点75bp(含)'
         left join tmp_fee_diff_details dq03
                   on a.branch_no = dq03.branch_no and
                      a.deposit_type = dq03.deposit_type and
                      (dq03.pro_type = '定期' or dq03.pro_type = '协定') and -- 定期/协定
                      dq03.rate_des = '两年及以内的加点不超过80个bp的个人大额存单'
         left join tmp_fee_diff_details wb01
                   on a.branch_no = wb01.branch_no and
                      a.deposit_type = wb01.deposit_type and
                      (wb01.pro_type = '外币一年期及以内' or wb01.pro_type = '外币活期') -- 外币一年期及以内/外币活期
order by a.branch_no, a.deposit_type
;
