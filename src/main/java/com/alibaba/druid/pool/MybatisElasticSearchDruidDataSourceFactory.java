package com.alibaba.druid.pool;

import javax.sql.DataSource;
import java.util.Map;
import java.util.Properties;

/**
 * Created by wjj on 2017/6/1.
 */
public class MybatisElasticSearchDruidDataSourceFactory extends DruidDataSourceFactory{
    @Override
    protected DataSource createDataSourceInternal(Properties properties) throws Exception {
        DruidDataSource dataSource = new MybatisElasticSearchDruidDataSource();
        config(dataSource, properties);
        return dataSource;
    }
    @SuppressWarnings("rawtypes")
    public static DataSource createDataSource(Properties properties) throws Exception {
        return createDataSource((Map) properties);
    }
    @SuppressWarnings("rawtypes")
    public static DataSource createDataSource(Map properties) throws Exception {
        DruidDataSource dataSource = new MybatisElasticSearchDruidDataSource();
        config(dataSource, properties);
        return dataSource;
    }
}
