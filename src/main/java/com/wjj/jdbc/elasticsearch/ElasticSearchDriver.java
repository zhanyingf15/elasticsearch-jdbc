package com.wjj.jdbc.elasticsearch;

import com.wjj.jdbc.util.LOG;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * @Author wangjiajun
 * @Date 2017/8/14 11:25
 */
public class ElasticSearchDriver implements Driver{
    org.slf4j.Logger logger = LOG.getLogger(ElasticSearchDriver.class);
    static {
        try {
            DriverManager.registerDriver(new ElasticSearchDriver());
        }catch (Exception e){

        }
    }
    public ElasticSearchDriver(){

    }
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        return new ElasticSearchConnection(url);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return true;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }
}
