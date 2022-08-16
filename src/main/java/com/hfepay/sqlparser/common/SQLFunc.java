package com.hfepay.sqlparser.common;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.util.StringUtils;

public class SQLFunc {
    public static SQLObject getParentObject(SQLObject x, Class<?> parentClass) {
        for (SQLObject parent = x.getParent(); parent != null; parent = parent.getParent()) {
            if (parentClass.isAssignableFrom(parent.getClass()))
                return parent;
        }
        return null;
    }


    public static SQLTableSource getParentTableSource(SQLObject x) {
        SQLObject parent = getParentObject(x, SQLTableSource.class);
        if (parent != null)
            return (SQLTableSource) parent;
        else
            return null;
    }

    public static SQLSelectItem getParentSelectItem(SQLExpr x) {
        for (SQLObject parent = x.getParent(); parent != null; parent = parent.getParent()) {
            if (parent instanceof SQLSelectItem) {
                return (SQLSelectItem) parent;
            }
        }
        return null;
    }

    public static boolean isMainSelectItem(SQLSelectItem x) {
        try {
            if (x
                    .getParent() //SQLSelectQueryBlock
                    .getParent() //SQLSelect
                    .getParent() //SQLSelectStatement
                    .getParent()
                    == null) {
                return true;
            }
        }catch (Exception e){}
        return false;
    }

    public static boolean isAllColumnExpr(SQLExpr expr) {
        return (expr instanceof SQLAllColumnExpr)
                || (expr instanceof SQLPropertyExpr && StringUtils.equals(((SQLPropertyExpr) expr).getName(), "*"));
    }

    public static boolean hasAllColumnExpr(SQLSelectStatement stmt) {
        return getAllColumnExprNumber(stmt) > 0;
    }

    public static int getAllColumnExprNumber(SQLSelectStatement statement) {
        int n = 0;
        try {
            for (SQLSelectItem item : statement.getSelect().getQueryBlock().getSelectList()) {
                if (isAllColumnExpr(item.getExpr())) {
                    n++;
                }
            }
        } catch (Exception e) {
            return 0;
        }
        return n;
    }

    public static SQLTableSource getExprTableSource(SQLExpr expr) {
        //TODO
        java.lang.reflect.Method method;
        try {
            method = expr.getClass().getMethod("getResolvedTableSource");
            return (SQLTableSource) method.invoke(expr);
        } catch (Exception e) {
            return null;
        }

    }

    public static String getItemAlias(SQLSelectItem item) {
        return item.getAlias() != null ?
                item.getAlias() :
                item.getExpr() instanceof SQLIdentifierExpr ?
                        ((SQLIdentifierExpr) item.getExpr()).getSimpleName() :
                        item.getExpr() instanceof SQLPropertyExpr ?
                                ((SQLPropertyExpr) item.getExpr()).getSimpleName() :
                                null;
    }

    public static boolean isSQLAggregateExpr(SQLExpr expr) {
        return expr instanceof SQLAggregateExpr || getParentObject(expr, SQLAggregateExpr.class) != null;
    }
}
