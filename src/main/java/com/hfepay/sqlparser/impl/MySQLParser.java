package com.hfepay.sqlparser.impl;

import com.alibaba.druid.DbType;

public class MySQLParser extends SQLParserImpl {
    public MySQLParser(String sql) {
        super(DbType.mysql, sql);
    }
}
