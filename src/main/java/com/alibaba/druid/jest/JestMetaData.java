package com.alibaba.druid.jest;

import com.alibaba.druid.util.jdbc.ResultSetMetaDataBase;
import io.searchbox.client.JestResult;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * Created by wjj on 2017/6/22.
 */
public class JestMetaData extends ResultSetMetaDataBase {
    public static final String COLUMN_LABEL = "jest_result";
    public static final String COLUMN_NAME = "jest_result";
    public JestMetaData(JestResult jestResult){
        super();
        List<ColumnMetaData> columns = getColumns();
        columns.clear();
        ColumnMetaData columnMetaData = new ColumnMetaData();
        columnMetaData.setColumnLabel(COLUMN_LABEL);
        columnMetaData.setColumnName(COLUMN_NAME);
        columns.add(columnMetaData);
    }
    @Override
    public int getColumnType(int column) throws SQLException {
        return Types.OTHER; //返回Types.OTHER，java会通过getObject方法获取
    }
    @Override
    public String getColumnClassName(int column) throws SQLException {
        return "java.lang.Object";
    }
}
