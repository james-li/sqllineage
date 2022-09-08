package com.hfepay.sqlparser.impl;

import com.alibaba.druid.DbType;

public class MysqlLinageSchemaStateVisitor extends DbLinageSchemaStateVisitor {


    public MysqlLinageSchemaStateVisitor() {
        super(DbType.mysql);
    }

}
