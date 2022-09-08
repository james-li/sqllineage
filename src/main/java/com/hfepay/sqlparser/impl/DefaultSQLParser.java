package com.hfepay.sqlparser.impl;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.parser.SQLStatementParser;

public class DefaultSQLParser extends SQLParserImpl {
    public DefaultSQLParser(DbType dbType, String sql) {
        super(dbType, sql);
    }

    @Override
    protected SQLStatementParser getSQLParser(String sql) {
        return null;
    }

    @Override
    protected DbLinageSchemaStateVisitor getDbLinageSchemaStateVisitor() {
        return new DbLinageSchemaStateVisitor(dbType);
    }
}
