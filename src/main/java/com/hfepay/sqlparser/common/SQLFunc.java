package com.hfepay.sqlparser.common;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.repository.SchemaRepository;
import com.alibaba.druid.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class SQLFunc {
    public static SQLObject getParentObject(SQLObject x, Class<?> parentClass) {
        for (SQLObject parent = x.getParent(); parent != null; parent = parent.getParent()) {
            if (parentClass.isAssignableFrom(parent.getClass()))
                return parent;
        }
        return null;
    }

    public static Object invokeMethod(SQLObject x, String method) {
        try {
            return x.getClass().getMethod(method).invoke(x);
        } catch (Exception e) {
            return null;
        }
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
            SQLSelectStatement s = (SQLSelectStatement) (x.getParent()
                    .getParent()
                    .getParent());
            if (s.getParent() == null) {
                return true;
            }
        } catch (Exception e) {
        }
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

    public static SQLTableSource getResolvedTableSource(SQLExpr expr) {
        try {
            return (SQLTableSource) invokeMethod(expr, "getResolvedTableSource");
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

    public static ArrayList<String> getExprTableSourceColumn(SchemaRepository repository, SQLExprTableSource tableSource) {
        SQLStatement stmt = repository.findTable(tableSource).getStatement();
        if (repository.getDbType() == DbType.hive && stmt instanceof SQLCreateTableStatement) {
            return ((SQLCreateTableStatement) stmt).getTableElementList().stream().map(Object::toString).collect(Collectors.toCollection(ArrayList::new));
        }
        return new ArrayList<>();
    }


    public static String getColumnName(SQLSelectItem item) throws ParserException {
        if (item.getAlias() != null) {
            return item.getAlias();
        } else if (item.getExpr() instanceof SQLIdentifierExpr) {
            return ((SQLIdentifierExpr) item.getExpr()).getName();
        } else if (item.getExpr() instanceof SQLPropertyExpr) {
            return ((SQLPropertyExpr) item.getExpr()).getName();
        } else {
            //throw new ParserException("Can not parse sql expr as column: " + item.getExpr().toString());
            return item.toString();
        }
    }

    public static String getName(SQLObject object) {
        try {
            return (String) invokeMethod(object, "getName");
        } catch (Exception e) {
            return "";
        }
    }

    public static String getSimpleName(SQLObject object) {
        try {
            return (String) invokeMethod(object, "getSimpleName");
        } catch (Exception e) {
            return "";
        }
    }


}
