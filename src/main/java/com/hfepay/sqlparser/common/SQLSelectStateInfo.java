package com.hfepay.sqlparser.common;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.stat.TableStat;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SQLSelectStateInfo {

    private Map<SQLObjectWrapper<SQLSelectItem>, HashSet<SQLObjectWrapper<SQLExpr>>> exprOfSelectItem = new HashMap<>();
    private Map<SQLObjectWrapper<SQLExpr>, HashSet<TableStat.Column>> exprOfColumnMap = new HashMap<>();


    public ArrayList<SQLExpr> getExprOfSelectItem(SQLSelectItem item) {
        return exprOfSelectItem.computeIfAbsent(new SQLObjectWrapper<SQLSelectItem>(item), v -> new HashSet<>()).stream()
                .map(x -> (SQLExpr) x.getObject())
                .collect(Collectors.toCollection(ArrayList::new));
    }


    public void addExprColumnMap(SQLExpr expr, TableStat.Column column) {
        if (expr == null)
            return;
        HashSet<TableStat.Column> columns = exprOfColumnMap.computeIfAbsent(new SQLObjectWrapper<>(expr), k -> new HashSet<>());
        columns.add(column);
    }

    public void addExprToSelectItem(SQLSelectItem item, SQLExpr expr) {
        SQLObjectWrapper<SQLExpr> sqlExprSQLObjectWrapper = new SQLObjectWrapper<>(expr);
        HashSet<SQLObjectWrapper<SQLExpr>> exprs = exprOfSelectItem.computeIfAbsent(new SQLObjectWrapper<>(item), k -> new HashSet<>());
        exprs.add(sqlExprSQLObjectWrapper);
    }


}
