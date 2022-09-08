package com.hfepay.sqlparser.impl;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.SQLStatementParser;

public class MysqlParser extends SQLParserImpl {
    public MysqlParser(String sql) {
        super(DbType.mysql, sql);

    }

    @Override
    protected SQLStatementParser getSQLParser(String sql) {
        return new MySqlStatementParser(sql);
    }

    @Override
    protected DbLinageSchemaStateVisitor getDbLinageSchemaStateVisitor() {
        return new MysqlLinageSchemaStateVisitor();
    }
}
