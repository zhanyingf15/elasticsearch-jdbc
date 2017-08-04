package com.alibaba.druid.pool;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by wjj on 2017/5/25.
 */
public class MybatisElasticSearchDruidPooledConnection extends ElasticSearchDruidPooledConnection {
    public MybatisElasticSearchDruidPooledConnection(DruidConnectionHolder holder){
        super(holder);
    }
    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkState();

        PreparedStatementHolder stmtHolder = null;
        DruidPooledPreparedStatement.PreparedStatementKey key = new DruidPooledPreparedStatement.PreparedStatementKey(sql, getCatalog(), PreparedStatementPool.MethodType.M1);

        boolean poolPreparedStatements = holder.isPoolPreparedStatements();

        if (poolPreparedStatements) {
            stmtHolder = holder.getStatementPool().get(key);
        }

        if (stmtHolder == null) {
            try {
                stmtHolder = new PreparedStatementHolder(key, conn.prepareStatement(sql));
                holder.getDataSource().incrementPreparedStatementCount();
            } catch (SQLException ex) {
                handleException(ex);
            }
        }

        initStatement(stmtHolder);

        DruidPooledPreparedStatement rtnVal = new MybatisElasticSearchDruidPooledPreparedStatement(this, stmtHolder);

        holder.addTrace(rtnVal);

        return rtnVal;
    }


    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        checkState();

        PreparedStatementHolder stmtHolder = null;
        DruidPooledPreparedStatement.PreparedStatementKey key = new DruidPooledPreparedStatement.PreparedStatementKey(sql, getCatalog(), PreparedStatementPool.MethodType.M2, resultSetType,
                resultSetConcurrency);

        boolean poolPreparedStatements = holder.isPoolPreparedStatements();

        if (poolPreparedStatements) {
            stmtHolder = holder.getStatementPool().get(key);
        }

        if (stmtHolder == null) {
            try {
                stmtHolder = new PreparedStatementHolder(key, conn.prepareStatement(sql, resultSetType,
                        resultSetConcurrency));
                holder.getDataSource().incrementPreparedStatementCount();
            } catch (SQLException ex) {
                handleException(ex);
            }
        }

        initStatement(stmtHolder);

        DruidPooledPreparedStatement rtnVal = new MybatisElasticSearchDruidPooledPreparedStatement(this, stmtHolder);

        holder.addTrace(rtnVal);

        return rtnVal;
    }
    private void initStatement(PreparedStatementHolder stmtHolder) throws SQLException {

        stmtHolder.incrementInUseCount();
        holder.getDataSource().initStatement(this, stmtHolder.getStatement());
    }
    /*@Override
    public void close() throws SQLException {
        *//*if (isDisable()) {
            return;
        }
        if(this.holder==null){
            return;
        }
        Iterator<Map.Entry<DruidPooledPreparedStatement.PreparedStatementKey, PreparedStatementHolder>> i = this.holder.getStatementPool().getMap().entrySet().iterator();
        while(i.hasNext()){
            Map.Entry<DruidPooledPreparedStatement.PreparedStatementKey, PreparedStatementHolder> e = i.next();
            PreparedStatementHolder psh = e.getValue();
            psh.getStatement().close();
        }
        this.holder.setDiscard(true);
        this.getConnection().close();*//*
        super.close();
//        DruidAbstractDataSource dataSource = holder.getDataSource();
        System.err.println("close:"+Thread.currentThread().getName());
//        System.err.println("active:"+dataSource.getActiveConnections().size());
    }*/

}
