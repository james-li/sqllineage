package com.hfepay.sqlparser.impl;

import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.hfepay.sqlparser.ISchemaRepository;
import com.hfepay.sqlparser.SQLParser;

import java.util.ArrayList;
import java.util.HashMap;

public class SQLParserImpl implements SQLParser {
    protected String dbType ;
    protected ISchemaRepository tableInfoSource = null;
    protected String sql ;

    public SQLParserImpl(String dbType, String sql) {
        this.dbType = dbType;
        this.sql = sql;
    }


    @Override
    public HashMap<String, ArrayList<SQLColumnDefinition>> analysisLineage() {
        return null;
    }

    @Override
    public ArrayList<String> getColumns() {
        return null;
    }

    public ISchemaRepository getTableInfoSource() {
        return tableInfoSource;
    }

    public void setTableInfoSource(ISchemaRepository tableInfoSource) {
        this.tableInfoSource = tableInfoSource;
    }
}
