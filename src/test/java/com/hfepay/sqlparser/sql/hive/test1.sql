select branch_no, bal
from (select *
      from (select branch_no, sum(bal) as bal
            from branch_bal
            group by branch_no
            union
            select lpad(branch_no, 16, '0'), avg(bal) as bal
            from branch_bal
            group by branch_no))