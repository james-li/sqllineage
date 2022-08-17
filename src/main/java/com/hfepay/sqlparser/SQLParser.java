package com.hfepay.sqlparser;

import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.stat.TableStat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

public interface SQLParser {
    LinkedHashMap<String, ArrayList<TableStat.Column>> analysisLineage();

    ArrayList<String> getColumns();

    default void setSchemaRepository(ISchemaRepository schemaRepository) {
    }
}
