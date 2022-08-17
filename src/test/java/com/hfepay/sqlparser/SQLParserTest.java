package com.hfepay.sqlparser;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.hive.parser.HiveStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.sql.repository.SchemaRepository;
import com.alibaba.druid.stat.TableStat;
import com.alibaba.druid.util.JdbcConstants;
import com.hfepay.sqlparser.common.SQLSelectStateInfo;
import com.hfepay.sqlparser.impl.HiveLinageSchemaStateVisitor;
import com.hfepay.sqlparser.impl.SQLParserImpl;
import junit.framework.TestCase;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.stream.Collectors;

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


    public void testHiveParser() {
        DbType dbType = DbType.hive;
        init(dbType);
        ISchemaRepository repository = new ISchemaRepository() {
            @Override
            public SchemaRepository getSchemaRepository(DbType dbType) {
                SchemaRepository schemaRepository = new SchemaRepository(dbType);
                for (String sql : initSQLs) {
                    schemaRepository.console(sql);
                }
                return schemaRepository;
            }
        };
        for (String sql : testSQLs) {
            SQLParser parser = SQLParserImpl.getSQLParser(dbType, sql);
            parser.setSchemaRepository(repository);
            System.out.println(sql);
            parser.analysisLineage().entrySet().stream().forEach(System.out::println);
        }
    }

    public void testHiveParser01() {
//        String sql = "select cust_id, '贷款' as type, sum(balance) as bal from loan_statistics group by cust_id " +
//                "union select cust_id, '存款' as type, sum(balance) as bal from deposit_statistics group by cust_id";
//        String sql = "select branch_no, a.etl_date as bdate, a.etl_date, (a.bal - COALESCE (b.bal, 0) - b.basic_bal) as bal from branch_bal a\n" +
//                "left join (select * from branch_bal_t  a where etl_date between '2022-01-01' and '2022-12-31') b on datediff(a.etl_date, b.etl_date) = 1";
        String sql = "select branch_no, bal from (select * from (select branch_no, sum(bal) as bal from branch_bal group by branch_no union select lpad(branch_no, 16,'0') , avg(bal) as bal from branch_bal group by branch_no))";
//        String sql = "select branch_no, c from (select branch_no, COUNT(bal) as c from branch_bal group by branch_no)";
//        String sql = "select branch_no, a.etl_date,  sum(balance) over(partition by branch_no, etl_date) as balance  from loan_statistics a;";
        SQLStatementParser parser = new HiveStatementParser(sql);
//        SQLStatement stmt = parser.parseStatementList().get(0);
        SQLSelectStatement stmt = (SQLSelectStatement) parser.parseStatementList().get(0);
        ISchemaRepository repository = new ISchemaRepository() {
            @Override
            public SchemaRepository getSchemaRepository(DbType dbType) {
                SchemaRepository schemaRepository = new SchemaRepository(dbType);
                schemaRepository.console("create table branch_bal(\n" +
                        "\tbranch_no string comment '机构号',\n" +
                        "\tbal decimal(18,2) comment '余额',\n" +
                        "\tetl_date date comment '日期'\n" +
                        ");");
                schemaRepository.console("create table branch_bal_t(\n" +
                        "\tbranch_no string comment '机构号',\n" +
                        "\tbal decimal(18,2) comment '余额',\n" +
                        "\tbasic_bal decimal(18,2) comment '基础余额',\n" +
                        "\tetl_date date comment '日期'\n" +
                        ");");
                return schemaRepository;
            }
        };
        HiveLinageSchemaStateVisitor visitor = new HiveLinageSchemaStateVisitor();
        visitor.setRepository(repository.getSchemaRepository(JdbcConstants.HIVE));
        stmt.accept(visitor);
        System.out.println(visitor.getColumns());
        visitor.getColumns();
        SQLSelectStateInfo selectStateInfo = visitor.getSelectStatInfo();


//        SQLParser sqlParser = new HiveParser(sql);
//        System.out.println(sqlParser.getColumns());

    }
}