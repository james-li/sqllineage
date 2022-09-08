package com.hfepay.sqlparser;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.repository.SchemaRepository;
import com.alibaba.druid.stat.TableStat;
import com.hfepay.sqlparser.impl.SQLParserImpl;
import junit.framework.TestCase;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;

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


    public static void testParser(DbType dbType) {
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
            parser.analysisLineage().entrySet().forEach(System.out::println);
        }
    }

    public void testParser() {
        for (DbType dbType : new DbType[]{DbType.hive, DbType.mysql}) {
            testParser(dbType);
        }
    }

}