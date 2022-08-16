package com.hfepay.sqlparser.impl;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.hive.parser.HiveStatementParser;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.util.JdbcConstants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class HiveParser extends SQLParserImpl {
    private List<SQLStatement> stmts = null;
    private SQLSelectStatement selectStmt = null;

    public HiveParser(String sql) {
        super(JdbcConstants.HIVE, sql);
        try {
            SQLStatementParser parser = new HiveStatementParser(sql);
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


    /**
     * 支持通过sql分析血缘关系，支持两种sql：
     * select xxx from xxxx
     * insert into(overwrite) table select xxx
     *
     * @return
     */

    public HashMap<String, ArrayList<SQLColumnDefinition>> analysisLineage() {

        return null;
    }

    /**
     * @return 返回sql的column列表，字段名称优先 字段别名，没有别名，则要求字段类型为SQLIdentifierExpr
     */
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
