# 数据血缘分析工具
## 简介
支持通过SQL分析，获取数据表的血缘关系，SQL可以是select/create table /insert overwrite/insert into类型，如下 
```sql
>select aaa from (select bbb from table);
>create table t1 select aaa from (select bbb from table);
>insert into table t1 select aaa from (select bbb from table);
```
返回结果是为`LinkedHashMap<String, ArrayList<TableStat.Column>>`，对应目标表字段名和源表字段名的一个映射关系

解析基于alibaba druid工具，通过SQL AST分析工具来对血缘进行分析。

## 使用方法
1. 首先加载源表meta信息，定义一个ISchemaRepository接口，加载源表的元数据信息，可以通过jdbc/redis/文件等，
来获取建表语句，下面是一个使用文件定义的示例：
```java
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
```
2. 使用SQLParser对sql进行分析
```java
    SQLParser parser = SQLParserImpl.getSQLParser(dbType, sql);
    parser.setSchemaRepository(repository);
    System.out.println(sql);
    parser.analysisLineage().entrySet().stream().forEach(System.out::println);

```

## TODO
增加mysql和其他数据库的支持，