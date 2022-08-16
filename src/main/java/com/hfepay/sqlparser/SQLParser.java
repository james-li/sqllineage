package com.hfepay.sqlparser;

import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;

import java.util.ArrayList;
import java.util.HashMap;

public interface SQLParser {
    HashMap<String, ArrayList<SQLColumnDefinition>> analysisLineage();
    ArrayList<String> getColumns();


}
