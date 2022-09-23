package com.hfepay.sqlparser;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.repository.SchemaRepository;
import com.alibaba.druid.stat.TableStat;
import com.hfepay.sqlparser.impl.SQLParserImpl;
import junit.framework.TestCase;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class SQLParserTest extends TestCase {

    private static ArrayList<String> initSQLs = new ArrayList<>();
    private static ArrayList<String> testSQLs = new ArrayList<>();

    private static LinkedHashMap<String, ArrayList<TableStat.Column>> analysisLineage(String sql, DbType dbType, ISchemaRepository repository) {
        SQLParser parser = SQLParserImpl.getSQLParser(dbType, sql);
        parser.setSchemaRepository(repository);
        return parser.analysisLineage();
    }

    public static void init(DbType dbType) {
        try {
            File sqlDir = new File("src/test/java/com/hfepay/sqlparser/sql/" + dbType.name());
            for (File sqlFile : sqlDir.listFiles()) {
                if (!sqlFile.getName().endsWith("sql")) {
                    continue;
                }
                String content = new String(Files.readAllBytes(Paths.get(sqlFile.getPath())));
                if (sqlFile.getName().startsWith("init")) {
                    initSQLs.add(content);
                } else if (sqlFile.getName().startsWith("test")) {
                    testSQLs.add(content);
                }
            }

        } catch (Exception e) {

        }
    }

    public static void parseSQL(String sql, DbType dbType, ISchemaRepository repository) {
        SQLParser parser = SQLParserImpl.getSQLParser(dbType, sql);
        parser.setSchemaRepository(repository);
        System.out.println(sql);
        parser.analysisLineage().entrySet().forEach(System.out::println);
    }


//    public static void parseSQL1(String sql, DbType dbType, ISchemaRepository iSchemaRepository) {
//        SchemaRepository repository = iSchemaRepository.getSchemaRepository(dbType);
//        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
//        assertEquals(1, stmtList.size());
//
//        SQLSelectStatement stmt = (SQLSelectStatement) stmtList.get(0);
//        repository.resolve(stmt);
//        SQLSelectQueryBlock queryBlock = stmt.getSelect().getQueryBlock();
//        for (SQLSelectItem item : queryBlock.getSelectList()) {
//            String columnName = SQLFunc.getColumnName(item);
//            SQLTableSource tableSource = queryBlock.findTableSourceWithColumn(columnName);
//            if (tableSource instanceof SQLExprTableSource) {
//
//            } else if (tableSource instanceof SQLSubqueryTableSource) {
//                SQLSelectQueryBlock subQueryBlock = ((SQLSubqueryTableSource) tableSource).getSelect().getQueryBlock();
//
//            }
//        }
//    }

    public static void testParser(DbType dbType) {
        init(dbType);
        ISchemaRepository repository = new ISchemaRepository() {
            private SchemaRepository schemaRepository = null;

            @Override
            public synchronized SchemaRepository getSchemaRepository(DbType dbType) {
                if (schemaRepository == null) {
                    schemaRepository = new SchemaRepository(dbType);
                    for (String sql : initSQLs) {
                        schemaRepository.console(sql);
                    }
                }
                return schemaRepository;
            }
        };
        for (String sql : testSQLs) {
            parseSQL(sql, dbType, repository);

        }
    }

    public void testParser() {
        //for (DbType dbType : new DbType[]{DbType.hive, DbType.mysql}) {
        for (DbType dbType : new DbType[]{DbType.hive}) {
            testParser(dbType);
        }
    }

    public void testRepository() {
        final DbType dbType = DbType.hive;

        SchemaRepository repository = new SchemaRepository(dbType);

        repository.console("create table t_emp(emp_id int, name string);");
        repository.console("create table t_org(org_id int, name string);");


        String sql = "select emp_id, emp_name, org_id, org_name from (SELECT emp_id, a.name AS emp_name, org_id, b.name AS org_name\n" +
                "FROM t_emp a\n" +
                "\tINNER JOIN t_org b ON a.emp_id = b.org_id) a INNER JOIN t_org b ON a.emp_id = b.org_id ";

        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
        assertEquals(1, stmtList.size());

        SQLSelectStatement stmt = (SQLSelectStatement) stmtList.get(0);
        SQLSelectQueryBlock queryBlock = stmt.getSelect().getQueryBlock();

// 大小写不敏感
        assertNotNull(queryBlock.findTableSource("A"));
        assertSame(queryBlock.findTableSource("a"), queryBlock.findTableSource("A"));

        //assertNull(queryBlock.findTableSourceWithColumn("emp_id"));

// 使用repository做column resolve
        repository.resolve(stmt);

        assertNotNull(queryBlock.findTableSourceWithColumn("emp_id"));

        SQLTableSource tableSource = queryBlock.findTableSourceWithColumn("emp_id");
        assertNotNull(((SQLExprTableSource) tableSource).getSchemaObject());

        SQLCreateTableStatement createTableStmt = (SQLCreateTableStatement) ((SQLExprTableSource) tableSource).getSchemaObject().getStatement();
        assertNotNull(createTableStmt);

        SQLSelectItem selectItem = queryBlock.findSelectItem("org_name");
        assertNotNull(selectItem);
        SQLPropertyExpr selectItemExpr = (SQLPropertyExpr) selectItem.getExpr();
        SQLColumnDefinition column = selectItemExpr.getResolvedColumn();
        assertNotNull(column);
        assertEquals("name", column.getName().toString());
        assertEquals("t_org", (((SQLCreateTableStatement) column.getParent()).getName().toString()));

        assertSame(queryBlock.findTableSource("B"), selectItemExpr.getResolvedTableSource());
    }

}