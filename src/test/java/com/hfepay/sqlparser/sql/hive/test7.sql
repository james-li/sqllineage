select branch_no, bal+100 as bal
from (
         with temp1 as (
             with temp1 as (select * from hxb_dh_data.loan_statistics)
             select branch_no, sum(balance) as bal
             from temp1
             group by branch_no
             union
             select lpad(branch_no, 8, '0') as b1, sum(balance) as bal1
             from temp1
             group by branch_no
         )
         select *
         from temp1
     );