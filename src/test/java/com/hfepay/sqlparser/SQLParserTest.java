package com.hfepay.sqlparser;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.hive.parser.HiveStatementParser;
import com.alibaba.druid.sql.dialect.hive.visitor.HiveSchemaStatVisitor;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import com.hfepay.sqlparser.common.SQLSelectStateInfo;
import com.hfepay.sqlparser.common.Tuple;
import com.hfepay.sqlparser.impl.HiveLinageSchemaStateVisitor;
import com.hfepay.sqlparser.impl.HiveParser;
import junit.framework.TestCase;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SQLParserTest extends TestCase {
    public void testHiveParser() {
//        String sql = "select cust_id, '贷款' as type, sum(balance) as bal from loan_statistics group by cust_id " +
//                "union select cust_id, '存款' as type, sum(balance) as bal from deposit_statistics group by cust_id";
        String sql = "select branch_no, a.etl_date as bdate, a.etl_date, (a.bal - COALESCE (b.bal, 0) - b.basic_bal) as bal from branch_bal a\n" +
                "left join (select a.bal, basic_bal, etl_date from branch_bal_t  a where etl_date between '2022-01-01' and '2022-12-31') b on datediff(a.etl_date, b.etl_date) = 1";
//        String sql = "select branch_no, a.etl_date,  sum(balance) over(partition by branch_no, etl_date) as balance  from loan_statistics a;";
        SQLStatementParser parser = new HiveStatementParser(sql);
//        SQLStatement stmt = parser.parseStatementList().get(0);
        SQLSelectStatement stmt = (SQLSelectStatement) parser.parseStatementList().get(0);

        HiveLinageSchemaStateVisitor visitor = new HiveLinageSchemaStateVisitor();
        stmt.accept(visitor);
        System.out.println(visitor.getColumns());
        visitor.getColumns();
        SQLSelectStateInfo selectStateInfo = visitor.getSelectStatInfo();
        System.out.println(selectStateInfo.getExprOfColumnMap());
        stmt.getSelect()
                .getQueryBlock()
                .getSelectList()
                .stream()
                .map(item -> item.toString() + "=====>" + selectStateInfo
                        .getExprOfSelectItem(item)
                        .stream()
                        .map(Object::toString)
                        .collect(Collectors.joining(",")))
                .forEach(System.out::println);
//        SQLParser sqlParser = new HiveParser(sql);
//        System.out.println(sqlParser.getColumns());

    }
}