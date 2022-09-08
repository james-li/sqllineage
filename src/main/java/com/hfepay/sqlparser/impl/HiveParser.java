package com.hfepay.sqlparser.impl;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.hive.parser.HiveStatementParser;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.stat.TableStat;
import com.hfepay.sqlparser.common.SQLObjectWrapper;
import com.hfepay.sqlparser.common.SQLSelectStateInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.stream.Collectors;

public class HiveParser extends SQLParserImpl {


    public HiveParser(String sql) {
        super(DbType.hive, sql);
    }


    @Override
    protected SQLStatementParser getSQLParser(String sql) {
        return new HiveStatementParser(sql);
    }


    @Override
    protected DbLinageSchemaStateVisitor getDbLinageSchemaStateVisitor() {
        return new HiveLinageSchemaStateVisitor();
    }


}
