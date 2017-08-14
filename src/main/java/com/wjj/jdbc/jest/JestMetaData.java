package com.wjj.jdbc.jest;

import io.searchbox.client.JestResult;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * Created by wjj on 2017/6/22.
 */
public class JestMetaData implements ResultSetMetaData {
    public static final String COLUMN_LABEL = "jest_result";
    public static final String COLUMN_NAME = "jest_result";
    public JestMetaData(JestResult jestResult){

    }
    @Override
    public int getColumnType(int column) throws SQLException {
        return Types.OTHER; //返回Types.OTHER，java会通过getObject方法获取
    }
    @Override
    public String getColumnClassName(int column) throws SQLException {
        return "java.lang.Object";
    }
    @Override
    public String getColumnLabel(int column) throws SQLException {
        return COLUMN_LABEL;
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return COLUMN_NAME;
    }
    @Override
    public int getColumnCount() throws SQLException {
        return 1;
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return 0;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return false;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return null;
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return null;
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return null;
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return null;
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
