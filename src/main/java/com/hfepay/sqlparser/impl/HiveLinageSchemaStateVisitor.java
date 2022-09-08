package com.hfepay.sqlparser.impl;

import com.alibaba.druid.DbType;

public class HiveLinageSchemaStateVisitor extends DbLinageSchemaStateVisitor {


    public HiveLinageSchemaStateVisitor() {
        super(DbType.hive);
    }

}
