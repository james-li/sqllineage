package com.hfepay.sqlparser.impl;

import com.alibaba.druid.util.JdbcConstants;

public class MySQLParser extends SQLParserImpl {
    public MySQLParser(String sql) {
        super(JdbcConstants.MYSQL, sql);
    }
}
