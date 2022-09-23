select
    branch_no
from
    (
        with with_tmp1 as
                 (
                     select
                         branch_no,
                         bal
                     from
                         (
                             with with_tmp2 as
                                      (
                                          select
                                              branch_no,
                                              balance as bal,
                                              etl_date
                                          from
                                              loan_statistics
                                      )
                             select
                                 *
                             from
                                 with_tmp2
                         )
                 )
        select
            *
        from
            with_tmp1
    );