package com.hfepay.sqlparser.impl;

import com.alibaba.druid.sql.ast.SQLDataType;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.hive.visitor.HiveSchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;
import com.hfepay.sqlparser.common.SQLFunc;
import com.hfepay.sqlparser.common.SQLSelectStateInfo;

import java.util.ArrayList;
import java.util.HashMap;

public class HiveLinageSchemaStateVisitor extends HiveSchemaStatVisitor {

    private SQLSelectStateInfo selectStatInfo = null;


    //在addColumn里对应expr和Column
    private SQLExpr currentExpr = null;

    public HiveLinageSchemaStateVisitor() {
        super();
        selectStatInfo = new SQLSelectStateInfo();
    }

    public SQLSelectStateInfo getSelectStatInfo() {
        return selectStatInfo;
    }

    protected TableStat.Column addColumn(String tableName, String columnName) {
        TableStat.Column column = super.addColumn(tableName, columnName);
        selectStatInfo.addExprColumnMap(currentExpr, column);
        return column;
    }

    protected TableStat.Column addColumn(SQLName table, String columnName) {
        TableStat.Column column = super.addColumn(table, columnName);
        selectStatInfo.addExprColumnMap(currentExpr, column);
        return column;
    }


    @Override
    public boolean visit(SQLPropertyExpr x) {
        currentExpr = x;
        boolean ret = super.visit(x);
        currentExpr = null;
        SQLSelectItem item = SQLFunc.getParentSelectItem(x);
        if (SQLFunc.isMainSelectItem(item)) {
            selectStatInfo.addExprToSelectItem(item, x);
        }
        return ret;
    }


    @Override
    public boolean visit(SQLIdentifierExpr x) {
        currentExpr = x;
        boolean ret = super.visit(x);
        currentExpr = null;
        SQLSelectItem item = SQLFunc.getParentSelectItem(x);
        if (SQLFunc.isMainSelectItem(item)) {
            selectStatInfo.addExprToSelectItem(item, x);
        }
        return ret;
    }

    @Override
    public void endVisit(SQLSelectStatement x) {
        super.endVisit(x);

    }


}
