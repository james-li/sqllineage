select branch_no, c
from (select branch_no, COUNT(bal) as c from branch_bal group by branch_no);