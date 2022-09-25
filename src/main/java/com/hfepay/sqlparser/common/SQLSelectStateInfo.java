package com.hfepay.sqlparser.common;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.repository.SchemaObject;
import com.alibaba.druid.sql.repository.SchemaRepository;
import com.alibaba.druid.stat.TableStat;
import com.alibaba.druid.util.StringUtils;

import java.util.*;
import java.util.stream.Collectors;

public class SQLSelectStateInfo {

    private Map<SQLObjectWrapper<SQLSelectItem>, ArrayList<SQLExpr>> exprOfSelectItem = new HashMap<>();
    private Map<SQLObjectWrapper<SQLSelectItem>, ArrayList<TableStat.Column>> tableLinageMap = new HashMap<>();
    private ArrayList<SQLSelectQueryBlock> queryBlocks = new ArrayList<>();
    private Map<SQLObjectWrapper<SQLSelectQueryBlock>, ArrayList<Tuple<SQLSelectItem, String>>> queryBlockColumns = new HashMap<>();
    private SchemaRepository repository = null;
    private ArrayList<SQLWithSubqueryClause.Entry> withSubQueries = new ArrayList<>();
    private HashMap<SQLObjectWrapper<SQLExprTableSource>, SQLWithSubqueryClause.Entry> withMap = new HashMap<>();


    public void setRepository(SchemaRepository repository) {
        this.repository = repository;
    }

    public Map<SQLObjectWrapper<SQLSelectItem>, ArrayList<SQLExpr>> getExprOfSelectItem() {
        return exprOfSelectItem;
    }

    public ArrayList<SQLExpr> getExprOfSelectItem(SQLSelectItem item) {
        return exprOfSelectItem
                .computeIfAbsent(new SQLObjectWrapper<>(item), v -> new ArrayList<>())
                .stream().map(SQLObjectWrapper::new)
                .collect(Collectors.toSet())
                .stream().map(x -> (SQLExpr) x.getObject())
                .collect(Collectors.toCollection(ArrayList::new));
    }


    public void addExprToSelectItem(SQLSelectItem item, SQLExpr expr) {
        ArrayList<SQLExpr> exprs = exprOfSelectItem.computeIfAbsent(new SQLObjectWrapper<>(item), k -> new ArrayList<>());
        exprs.add(expr);
    }

    public void addWithSubQuery(SQLWithSubqueryClause.Entry queryEntry) {
        withSubQueries.add(queryEntry);
    }

    public SQLWithSubqueryClause.Entry findWithSubQuery(SQLExprTableSource tableSource) {
        String tableName = ((SQLExprTableSource) tableSource).getTableName();
        for (SQLWithSubqueryClause.Entry entry : withSubQueries) {
            if (StringUtils.equalsIgnoreCase(tableName, entry.getAlias())) {
                return entry;
            }
        }
        return null;
    }

 /*   public HashSet<TableStat.Column> getExprLineage(SQLExpr expr) {
        HashSet<TableStat.Column> columns = new HashSet<>();
        SQLTableSource tableSource = null;
        if (expr instanceof SQLIdentifierExpr) {
            tableSource = ((SQLIdentifierExpr) expr).getResolvedTableSource();
        } else if (expr instanceof SQLPropertyExpr) {
            tableSource = ((SQLPropertyExpr) expr).getResolvedTableSource();
        } else if (expr instanceof SQLAllColumnExpr) {
            tableSource = ((SQLAllColumnExpr) expr).getResolvedTableSource();
        }
        if (tableSource == null)
            return columns;
        if (tableSource instanceof SQLExprTableSource) {
            String tableName = ((SQLExprTableSource) tableSource).getTableName();
            columns.add(new TableStat.Column(tableName, ((SQLName) expr).getSimpleName()));
        } else {
            ArrayList<SQLSelectQueryBlock> blocks = new ArrayList<>();
            //此处对union query做一个排序，第一个放在前面，取第一个的column name作为
            if (tableSource instanceof SQLUnionQueryTableSource) {
                blocks.add(((SQLUnionQueryTableSource) tableSource).getUnion().getFirstQueryBlock());
            }
            for (SQLSelectQueryBlock queryBlock : queryBlocks) {
                if (tableSource == SQLFunc.getParentObject(queryBlock, SQLTableSource.class) &&
                        !(!blocks.isEmpty() && queryBlock.equals(blocks.get(0)))) {
                    blocks.add(queryBlock);
                }
            }
            int[] idx = new int[1];
            idx[0] = -1;
            for (SQLSelectQueryBlock queryBlock : blocks) {
                HashSet<TableStat.Column> matchedColumns = searchColumnsInTableSource(queryBlock, ((SQLName) expr).getSimpleName(), idx);
                columns.addAll(matchedColumns);
            }
        }
        return columns;
    }

    private HashSet<TableStat.Column> searchColumnsInTableSource(SQLSelectQueryBlock queryBlock, String exprName, int[] idx) {
        SQLSelectItem selectItem = null;
        if (idx[0] == -1) {
            selectItem = findSelectItem(queryBlock, exprName, idx);
        } else {
            selectItem = queryBlock.getSelectItem(idx[0]);
        }

        ArrayList<SQLExpr> exprs = new ArrayList<>();
        for (SQLExpr x : exprOfSelectItem.getOrDefault(new SQLObjectWrapper<>(selectItem), new ArrayList<>())) {
            if (SQLFunc.isAllColumnExpr(x)) {
                SQLIdentifierExpr x1 = new SQLIdentifierExpr(exprName);
                x1.setResolvedTableSource(SQLFunc.getResovledTableSource(x));
                x1.setParent(x.getParent());
                exprs.add(x1);
            } else {
                exprs.add(x);
            }
        }
        HashSet<TableStat.Column> columns = new HashSet<>();
        for (SQLExpr x : exprs) {
            columns.addAll(getExprLineage(x));
        }
        return columns;
    }


    private SQLSelectItem findSelectItem(SQLSelectQueryBlock queryBlock, String columnName, int[] idx) {
        ArrayList<Tuple<SQLSelectItem, String>> columnNames = getColumns(queryBlock);
        idx[0] = -1;
        int i = 0;
        for (Tuple<SQLSelectItem, String> x : columnNames) {
            if (x.getVal2().equalsIgnoreCase(columnName)) {
                SQLSelectItem val1 = x.getVal1();
                idx[0] = i;
                return val1;
            }
            i++;
        }
        return null;
    }*/


    public HashSet<TableStat.Column> getExprLineage(SQLExpr expr, SQLTableSource tableSource) {
        HashSet<TableStat.Column> columns = new HashSet<>();
        if (tableSource == null) {
            tableSource = ((SQLSelectQueryBlock) SQLFunc.getParentObject(expr, SQLSelectQueryBlock.class)).getFrom();
        }
        String exprName = SQLFunc.getName(expr);
        if (StringUtils.isEmpty(exprName)) {
            return columns;
        }
        if (tableSource instanceof SQLExprTableSource) {
            String tableName = ((SQLExprTableSource) tableSource).getTableName();
            SQLWithSubqueryClause.Entry entry = withMap.get(new SQLObjectWrapper<>(tableSource));
            if (entry != null) {
                SQLSubqueryTableSource subqueryTableSource = new SQLSubqueryTableSource(entry.getSubQuery());
                columns.addAll(getExprLineage(expr, subqueryTableSource));
            } else {
                SQLColumnDefinition column = repository.findColumn(tableSource, expr);
                if (column != null) {
                    columns.add(new TableStat.Column(tableName, column.getColumnName()));
                } else {
                    columns.add(new TableStat.Column(tableName, exprName));
                }
            }
        } else if (tableSource instanceof SQLJoinTableSource) {
            HashSet<TableStat.Column> c = getExprLineage(expr, ((SQLJoinTableSource) tableSource).getLeft());
            if (c.size() == 0) {
                c = getExprLineage(expr, ((SQLJoinTableSource) tableSource).getRight());
            }
            columns.addAll(c);
        } else if (tableSource instanceof SQLSubqueryTableSource) {
            SQLSubqueryTableSource subqueryTableSource = (SQLSubqueryTableSource) tableSource;
            SQLTableSource from = subqueryTableSource.getSelect().getQueryBlock().getFrom();
            HashMap<SQLObjectWrapper<SQLSelectItem>, SQLTableSource> allColumnTable = new HashMap<>();
            for (SQLSelectItem item : subqueryTableSource.getSelect().getQueryBlock().getSelectList()) {
                if (StringUtils.equals(SQLFunc.getColumnName(item), exprName)) {
                    columns.addAll(getExprLineage(item, from));
                    allColumnTable.clear();
                    break;
                }
                if (item.getExpr() instanceof SQLAllColumnExpr ||
                        (item.getExpr() instanceof SQLIdentifierExpr && StringUtils.equalsIgnoreCase("*", ((SQLIdentifierExpr) item.getExpr()).getName()))) {
                    SQLTableSource itemTableSource = SQLFunc.getResolvedTableSource(item.getExpr());
                    if (itemTableSource != null)
                        allColumnTable.put(new SQLObjectWrapper<>(item), itemTableSource);
                }

            }
            for (SQLTableSource tSource : allColumnTable.values()) {
                HashSet<TableStat.Column> c = getExprLineage(expr, tSource);
                if (c.size() > 0) {
                    columns.addAll(c);
                    break;
                }
            }
        } else if (tableSource instanceof SQLUnionQueryTableSource) {
            int idx = -1;
            SQLUnionQueryTableSource unionQueryTableSource = (SQLUnionQueryTableSource) tableSource;
            for (SQLSelectQuery query : unionQueryTableSource.getUnion().getChildren()) {
                if (!(query instanceof SQLSelectQueryBlock)) {
                    continue;
                }
                SQLSelectQueryBlock block = (SQLSelectQueryBlock) query;
                SQLSelectItem matchedItem = null;
                if (idx == -1) {
                    matchedItem = block.findSelectItem(exprName);
                    idx = block.getSelectList().indexOf(matchedItem);
                } else {
                    /**
                     * FIXME:
                     * select a,b,c from t1 union select * from t2
                     */
                    matchedItem = block.getSelectItem(idx);
                }
                if (matchedItem != null)
                    columns.addAll(getExprLineage(matchedItem, block.getFrom()));
            }
        } else if (tableSource instanceof SQLWithSubqueryClause.Entry) {
            SQLWithSubqueryClause.Entry with = (SQLWithSubqueryClause.Entry) tableSource;
            SQLSelectQuery query = with.getSubQuery().getQuery();
            if (query instanceof SQLUnionQuery)
                columns.addAll(getExprLineage(expr, new SQLUnionQueryTableSource((SQLUnionQuery) query)));
            else
                columns.addAll(getExprLineage(expr, new SQLSubqueryTableSource(query)));
        }

        return columns;
    }

    public HashSet<TableStat.Column> getExprLineage(SQLSelectItem item) {
        return getExprLineage(item, null);
    }

    public HashSet<TableStat.Column> getExprLineage(SQLSelectItem item, SQLTableSource tableSource) {
        HashSet<TableStat.Column> columns = new HashSet<>();
        ArrayList<SQLExpr> exprs = exprOfSelectItem.get(new SQLObjectWrapper<>(item));
        if (exprs == null) {
            return columns;
        }
        SQLSelectQueryBlock block = (SQLSelectQueryBlock) SQLFunc.getParentObject(item, SQLSelectQueryBlock.class);
        if (block == null)
            return columns;
        if (tableSource == null) {
            tableSource = block.getFrom();
        }
        for (SQLExpr x : exprs) {
            SQLAggregateExpr xAggExpr = (SQLAggregateExpr) SQLFunc.getParentObject(x, SQLAggregateExpr.class);
            if (xAggExpr != null && xAggExpr.getMethodName().equalsIgnoreCase("count"))
                continue;
            String exprName = SQLFunc.getName(x);
            SQLTableSource from = tableSource;
            if (x instanceof SQLAllColumnExpr ||
                    (x instanceof SQLIdentifierExpr && "*".equals(((SQLIdentifierExpr) x).getName()))) {
                from = SQLFunc.getResolvedTableSource(x);
                if (from == null) {
                    from = tableSource;
                }
                SQLIdentifierExpr alt = new SQLIdentifierExpr(exprName);
                alt.setParent(x.getParent());
                x = alt;
            }
            columns.addAll(getExprLineage(x, from));
        }
        return columns;
    }


/*

    public HashSet<TableStat.Column> getExprLineage(SQLExpr expr, SQLTableSource tableSource) {
        HashSet<TableStat.Column> columns = new HashSet<>();
        String exprName = "";
        if (expr instanceof SQLIdentifierExpr) {
            exprName = ((SQLIdentifierExpr) expr).getName();
            if (tableSource == null)
                tableSource = ((SQLIdentifierExpr) expr).getResolvedTableSource();
        } else if (expr instanceof SQLPropertyExpr) {
            exprName = ((SQLPropertyExpr) expr).getName();
            if (tableSource == null)
                tableSource = ((SQLPropertyExpr) expr).getResolvedTableSource();
        }
        if ("".equals(exprName)) {
            return columns;
        }
        if (tableSource == null) {
            SQLSelectQueryBlock queryBlock = (SQLSelectQueryBlock) SQLFunc.getParentObject(expr, SQLSelectQueryBlock.class);
            tableSource = queryBlock.findTableSourceWithColumn(exprName);
        }
        if (tableSource instanceof SQLExprTableSource) {
            String tableName = ((SQLExprTableSource) tableSource).getTableName();
            SQLWithSubqueryClause.Entry entry = withMap.get(new SQLObjectWrapper<>(tableSource));
            if (entry != null) {
                SQLSelectQueryBlock queryBlock = entry.getSubQuery().getQueryBlock();
                for (SQLSelectItem item : queryBlock.getSelectList()) {
                    if (StringUtils.equalsIgnoreCase(SQLFunc.getColumnName(item), exprName)) {
                        columns.addAll(getExprLineage(item));
                        break;
                    }
                }
            } else {
                columns.add(new TableStat.Column(tableName, ((SQLName) expr).getSimpleName()));
            }
        } else {
            ArrayList<SQLSelectQueryBlock> blocks = new ArrayList<>();
            //此处对union query做一个排序，第一个放在前面，取第一个的column name作为
            if (tableSource instanceof SQLUnionQueryTableSource) {
                blocks.addAll(((SQLUnionQueryTableSource) tableSource)
                        .getUnion()
                        .getRelations()
                        .stream()
                        .map(x -> (SQLSelectQueryBlock) x)
                        .collect(Collectors.toList()));
            } else if (tableSource instanceof SQLSubqueryTableSource) {
                blocks.add(((SQLSubqueryTableSource) tableSource).getSelect().getQueryBlock());
            } else if (tableSource instanceof SQLWithSubqueryClause.Entry) {
                SQLSelectQuery query = ((SQLWithSubqueryClause.Entry) tableSource).getSubQuery().getQuery();
                if (query instanceof SQLUnionQuery) {
                    blocks.addAll(((SQLUnionQuery) query)
                            .getRelations()
                            .stream()
                            .map(x -> (SQLSelectQueryBlock) x)
                            .collect(Collectors.toList()));
                } else if (query instanceof SQLSelectQueryBlock) {
                    blocks.add((SQLSelectQueryBlock) query);
                }
            }
            int idx = -1;
            for (SQLSelectQueryBlock block : blocks) {
                SQLSelectItem item;
                if (idx == -1) {
                    item = block.findSelectItem(exprName);
                    idx = block.getSelectList().indexOf(item);
                } else {
                    item = block.getSelectList().get(idx);
                }
                if (item == null) {
                    continue;
                }
                columns.addAll(getExprLineage(item));
            }
        }
        return columns;
    }
*/

    public void addQueryBlock(SQLSelectQueryBlock queryBlock) {
        this.queryBlocks.add(queryBlock);
    }


    private ArrayList<Tuple<SQLSelectItem, String>> getColumns(SQLSelectQueryBlock queryBlock) {
        ArrayList<Tuple<SQLSelectItem, String>> columns = queryBlockColumns.get(new SQLObjectWrapper<>(queryBlock));
        if (columns != null) {
            return columns;
        }
        columns = new ArrayList<>();
        for (SQLSelectItem item : queryBlock.getSelectList()) {
            SQLExpr expr = item.getExpr();
            if (SQLFunc.isAllColumnExpr(expr)) {
                try {
                    //SQLAllColumnExpr & SQLPropertyExpr 都有 getResolvedTableSource方法
                    SQLTableSource tableSource = SQLFunc.getResolvedTableSource(expr);
                    final SQLSelectItem finalItem = item;
                    if (tableSource instanceof SQLExprTableSource) {
                        columns.addAll(SQLFunc.getExprTableSourceColumn(repository, (SQLExprTableSource) tableSource)
                                .stream()
                                .map(x -> new Tuple<>(finalItem, x))
                                .collect(Collectors.toList()));

                    } else if (tableSource instanceof SQLSubqueryTableSource) {
                        columns.addAll(getColumns(((SQLSubqueryTableSource) tableSource)
                                .getSelect()
                                .getQueryBlock())
                                .stream()
                                .map(x -> new Tuple<>(finalItem, x.getVal2()))
                                .collect(Collectors.toList()));
                    } else if (tableSource instanceof SQLUnionQueryTableSource) {
                        columns.addAll(getColumns(((SQLUnionQueryTableSource) tableSource)
                                .getUnion()
                                .getFirstQueryBlock())
                                .stream()
                                .map(x -> new Tuple<>(finalItem, x.getVal2()))
                                .collect(Collectors.toList()));
                    }
                } catch (Exception e) {
                }

            } else {
                columns.add(new Tuple<>(item, item.computeAlias()));

            }
        }
        return columns;
    }

    private void resolveQueryColumns() {
        for (SQLSelectQueryBlock block : queryBlocks) {
            queryBlockColumns.put(new SQLObjectWrapper<>(block), getColumns(block));
        }
    }

    public void resolveLineage() {
        resolveQueryColumns();
        tableLinageMap = exprOfSelectItem
                .keySet()
                .stream()
                .filter(x -> SQLFunc.isMainSelectItem((SQLSelectItem) x.getObject()))
                .collect(Collectors.toMap(
                        x -> x,
                        x -> new ArrayList<>(getExprLineage((SQLSelectItem) x.getObject()))
                ))
        ;
    }

    public Map<SQLObjectWrapper<SQLSelectItem>, ArrayList<TableStat.Column>> getTableLinageMap() {
        return tableLinageMap;
    }

    public void addWithMap(SQLExprTableSource x, SQLWithSubqueryClause.Entry withSubQuery) {
        withMap.put(new SQLObjectWrapper<>(x), withSubQuery);
    }
}
