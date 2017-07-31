package com.alibaba.druid.pool;

import com.alibaba.druid.util.jdbc.ResultSetMetaDataBase;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * Created by wjj on 2017/5/26.
 */
public class MybatisElasticSearchResultSetMetaDataBase extends ResultSetMetaDataBase {
    public MybatisElasticSearchResultSetMetaDataBase(List<String> headers){
        super();
        List<ColumnMetaData> columns = getColumns();
        columns.clear();
        for(String column:headers){
            ColumnMetaData columnMetaData = new ColumnMetaData();
            columnMetaData.setColumnLabel(column);
            columnMetaData.setColumnName(column);
            columns.add(columnMetaData);
        }
    }
    @Override
    public int getColumnType(int column) throws SQLException {
        return Types.LONGVARCHAR;
    }
    @Override
    public String getColumnClassName(int column) throws SQLException {
        return "java.lang.String";
    }
}
