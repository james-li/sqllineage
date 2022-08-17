create table branch_bal_t
(
    branch_no string comment '机构号',
    bal       decimal(18, 2) comment '余额',
    basic_bal decimal(18, 2) comment '基础余额',
    etl_date  date comment '日期'
);