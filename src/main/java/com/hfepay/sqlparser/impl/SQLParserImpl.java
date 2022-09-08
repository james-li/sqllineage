package com.hfepay.sqlparser.impl;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.stat.TableStat;
import com.hfepay.sqlparser.ISchemaRepository;
import com.hfepay.sqlparser.SQLParser;
import com.hfepay.sqlparser.common.SQLObjectWrapper;
import com.hfepay.sqlparser.common.SQLSelectStateInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

public abstract class SQLParserImpl implements SQLParser {
    protected DbType dbType;
    protected ISchemaRepository schemaRepository = null;
    protected String sql;
    protected List<SQLStatement> stmts = null;
    protected SQLSelectStatement selectStmt = null;

    public SQLParserImpl(DbType dbType, String sql) {
        this.dbType = dbType;
        this.sql = sql;
        try {
            SQLStatementParser parser = getSQLParser(sql);
            this.stmts = parser.parseStatementList();
            SQLStatement stmt = stmts.get(0);
            if (stmt instanceof SQLSelectStatement) {
                selectStmt = (SQLSelectStatement) stmt;
            } else if (stmt instanceof SQLCreateTableStatement) {
                selectStmt = new SQLSelectStatement(((SQLCreateTableStatement) stmt).getSelect());
            } else if (stmt instanceof SQLInsertStatement) {
                selectStmt = new SQLSelectStatement(((SQLInsertStatement) stmt).getQuery());
            }
        } catch (ParserException e) {
            stmts = null;
            selectStmt = null;
        }
    }

    public static SQLParser getSQLParser(DbType dbType, String sql) {
        switch (dbType) {
            case hive:
                return new HiveParser(sql);
            case mysql:
                return new MysqlParser(sql);
            default:
                return new DefaultSQLParser(dbType, sql);
        }
    }

    protected abstract SQLStatementParser getSQLParser(String sql);


    /**
     * 支持通过sql分析血缘关系，支持两种sql：
     * select xxx from xxxx
     * insert into(overwrite) table select xxx
     *
     * @return map[select字段，源表]
     */
    @Override
    public LinkedHashMap<String, ArrayList<TableStat.Column>> analysisLineage() {
//        HiveLinageSchemaStateVisitor visitor = new HiveLinageSchemaStateVisitor();
        DbLinageSchemaStateVisitor visitor = getDbLinageSchemaStateVisitor();
        if (visitor == null)
            return null;
        visitor.setRepository(this.schemaRepository.getSchemaRepository(DbType.hive));
        this.selectStmt.accept(visitor);
        SQLSelectStateInfo selectStateInfo = visitor.getSelectStatInfo();
        HashMap<SQLObjectWrapper<SQLSelectItem>, ArrayList<TableStat.Column>> tableLinageMap =
                (HashMap<SQLObjectWrapper<SQLSelectItem>, ArrayList<TableStat.Column>>) selectStateInfo.getTableLinageMap();
        LinkedHashMap<String, ArrayList<TableStat.Column>> tableLinage = new LinkedHashMap<>();
        for (SQLSelectItem item : selectStmt.getSelect().getQueryBlock().getSelectList()) {
            tableLinage.put(item.toString(), tableLinageMap.get(new SQLObjectWrapper<>(item)));
        }
        return tableLinage;
    }

    protected abstract DbLinageSchemaStateVisitor getDbLinageSchemaStateVisitor();


    public void setSchemaRepository(ISchemaRepository schemaRepository) {
        this.schemaRepository = schemaRepository;
    }

    /**
     * @return 返回sql的column列表，字段名称优先 字段别名，没有别名，则要求字段类型为SQLIdentifierExpr
     */
    @Override
    public ArrayList<String> getColumns() {
        if (selectStmt == null)
            return null;
        try {
            SQLSelectStatement stmt = selectStmt;
            return stmt.getSelect().getQueryBlock().getSelectList().stream().map(item -> {
                if (item.getAlias() != null) {
                    return item.getAlias();
                } else if (item.getExpr() instanceof SQLIdentifierExpr) {
                    return ((SQLIdentifierExpr) item.getExpr()).getName();
                } else if (item.getExpr() instanceof SQLPropertyExpr) {
                    return ((SQLPropertyExpr) item.getExpr()).getName();
                } else {
                    throw new ParserException("Can not parse sql expr as column: " + item.getExpr().toString());
                }
            }).collect(Collectors.toCollection(ArrayList::new));
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }

}
