package com.hfepay.sqlparser;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.repository.SchemaRepository;

public interface ISchemaRepository {
    /**
     * 通过表名获取表meta信息的接口，可以通过json配置文件或者从redis里获取
     * 或者通过jdbc获取
     *
     * @param dbType: 数据库类型，来自于DbType
     * @return 表字段列表
     */
    SchemaRepository getSchemaRepository(DbType dbType);
}
