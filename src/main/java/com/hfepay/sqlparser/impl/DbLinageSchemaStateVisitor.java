package com.hfepay.sqlparser.impl;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.repository.SchemaRepository;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import com.hfepay.sqlparser.common.SQLFunc;
import com.hfepay.sqlparser.common.SQLSelectStateInfo;

public class DbLinageSchemaStateVisitor extends SchemaStatVisitor {

    private SQLSelectStateInfo selectStatInfo = null;
    private DbType dbType;


    //在addColumn里对应expr和Column
    private SQLExpr currentExpr = null;

    public DbLinageSchemaStateVisitor(DbType dbType) {
        super();
        this.dbType = dbType;
        selectStatInfo = new SQLSelectStateInfo();
    }

    public SQLSelectStateInfo getSelectStatInfo() {
        return selectStatInfo;
    }

    @Override
    public void setRepository(SchemaRepository repository) {
        super.setRepository(repository);
        selectStatInfo.setRepository(repository);
    }


    @Override
    public boolean visit(SQLPropertyExpr x) {
        return visitSingleExpr(x);
    }

    @Override
    public boolean visit(SQLAllColumnExpr x) {
        return visitSingleExpr(x);
    }

    @Override
    public boolean visit(SQLIdentifierExpr x) {
        return visitSingleExpr(x);
    }

    private boolean visitSingleExpr(SQLExpr x) {
        currentExpr = x;
        boolean ret = false;
        if (x instanceof SQLIdentifierExpr) {
            ret = super.visit((SQLIdentifierExpr) x);
        } else if (x instanceof SQLPropertyExpr) {
            ret = super.visit((SQLPropertyExpr) x);
        } else if (x instanceof SQLAllColumnExpr) {
            ret = super.visit((SQLAllColumnExpr) x);
        }
        currentExpr = null;
        SQLSelectItem item = SQLFunc.getParentSelectItem(x);
        if (item != null) {
            SQLAggregateExpr expr = (SQLAggregateExpr) SQLFunc.getParentObject(x, SQLAggregateExpr.class);
            //count(a) as c 不分析
            if (expr == null || !expr.getMethodName().equalsIgnoreCase("count")) {
                selectStatInfo.addExprToSelectItem(item, x);
            }
        }
        return ret;
    }

    @Override
    public boolean visit(SQLSelectQueryBlock x) {
        boolean ret = super.visit(x);
        //获取所有tablesource的column信息
        selectStatInfo.addQueryBlock(x);
        return ret;
    }

    @Override
    public void endVisit(SQLSelectStatement x) {
        super.endVisit(x);
        selectStatInfo.resolveLineage();
    }
}
