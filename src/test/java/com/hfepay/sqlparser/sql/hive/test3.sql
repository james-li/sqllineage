select (oasis_dw.acct_bal.bal) as yzzb_0914,
       (prod_code)             as dim_prod,
       (org_code)              as dim_org,
       (acct_type_code)        as dim_acct_type,
       (acct_code)             as dim_acct,
       DATE_FORMAT(etl_date, 'yyyy-MM') as month
from oasis_dw.acct_bal
where 1=1
order by DATE_FORMAT(etl_date, 'yyyy-MM') desc