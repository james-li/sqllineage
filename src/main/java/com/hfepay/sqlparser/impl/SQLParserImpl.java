package com.hfepay.sqlparser.impl;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.stat.TableStat;
import com.hfepay.sqlparser.ISchemaRepository;
import com.hfepay.sqlparser.SQLParser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class SQLParserImpl implements SQLParser {
    protected DbType dbType;
    protected ISchemaRepository schemaRepository = null;
    protected String sql;

    public SQLParserImpl(DbType dbType, String sql) {
        this.dbType = dbType;
        this.sql = sql;
    }

    public static SQLParser getSQLParser(DbType dbType, String sql) {
        switch (dbType) {
            case hive:
                return new HiveParser(sql);
            default:
                return new SQLParserImpl(dbType, sql);
        }
    }


    @Override
    public LinkedHashMap<String, ArrayList<TableStat.Column>> analysisLineage() {
        return null;
    }

    @Override
    public ArrayList<String> getColumns() {
        return null;
    }

    public void setSchemaRepository(ISchemaRepository schemaRepository) {
        this.schemaRepository = schemaRepository;
    }

}
