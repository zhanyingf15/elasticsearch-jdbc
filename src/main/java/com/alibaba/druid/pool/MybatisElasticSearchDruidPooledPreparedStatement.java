package com.alibaba.druid.pool;

import com.alibaba.druid.jest.JestResultSet;
import com.alibaba.druid.jest.JestType;
import com.alibaba.druid.jest.JestUtil;
import com.alibaba.druid.proxy.jdbc.ConnectionProxyImpl;
import com.alibaba.druid.support.logging.Log;
import com.alibaba.druid.support.logging.LogFactory;
import io.searchbox.client.JestResult;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.plugin.nlpcn.QueryActionElasticExecutor;
import org.elasticsearch.plugin.nlpcn.executors.CsvExtractorException;
import org.nlpcn.es4sql.SearchDao;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.jdbc.ObjectResult;
import org.nlpcn.es4sql.jdbc.ObjectResultsExtractor;
import org.nlpcn.es4sql.query.QueryAction;

import java.lang.reflect.Method;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by wjj on 2017/5/25.
 */
public class MybatisElasticSearchDruidPooledPreparedStatement extends DruidPooledPreparedStatement {
    private final static Log LOG = LogFactory.getLog(MybatisElasticSearchDruidPooledPreparedStatement.class);
    LinkedList<String> sqlList = new LinkedList<String>();//存放分解的sql片段
    Client client = null;
    public MybatisElasticSearchDruidPooledPreparedStatement(DruidPooledConnection conn, PreparedStatementHolder holder)throws SQLException {
        super(conn,holder);
        this.client = ((ElasticSearchConnection)getOriginalConnection(conn.getConnection())).getClient();
        sqlList.clear();
        buildSqlList(getSql());

    }
    private Connection getOriginalConnection(Connection conn){
        if(conn instanceof ConnectionProxyImpl){
            return ((ConnectionProxyImpl)conn).getConnectionRaw();
        }else{//conn instanceof ElasticSearchConnection
            return conn;
        }
    }
    //分解sql，如select * from tableName where a=? and b=?分解成 ["select * from tableName where a=","?"," and b=","?"]
    private void buildSqlList(String sql){
        int index = sql.indexOf("?");
        if(index<0){
            sqlList.add(sql);
            return;
        }
        String preStr = sql.substring(0,index);
        sqlList.add(preStr);
        sqlList.add("?");
        String afterStr = sql.substring(index+1);
        buildSqlList(afterStr);
    }
    //下面setXXX方法用于替换?参数
    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        sqlList.set(2*parameterIndex-1,String.valueOf(x));
    }
    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        sqlList.set(2*parameterIndex-1,"'"+x+"'");
    }
    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        sqlList.set(2*parameterIndex-1,String.valueOf(x));
    }
    @Override
    public void setDate(int parameterIndex, java.sql.Date x) throws SQLException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        setString(parameterIndex,sdf.format(x));
    }
    @Override
    public void close() throws SQLException {
        sqlList.clear();//做下清空操作
        super.close();
    }
    @Override
    public ResultSet executeQuery() throws SQLException {
        String sql = getSql();
        checkOpen();
        incrementExecuteCount();
        transactionRecord(sql);
        oracleSetRowPrefetch();
        conn.beforeExecute();
        try {
            if(isExecuteSql(sql)){  //执行sql
                ObjectResult extractor = getObjectResult(true, StringUtils.join(sqlList.toArray(),""), false, false, true);
                List<String> headers = extractor.getHeaders();
                List<List<Object>> lines = extractor.getLines();
                ResultSet rs = new MybatisElasticSearchResultSet(this, headers, lines);
                if (rs == null) {
                    return null;
                }
                DruidPooledResultSet poolableResultSet = new DruidPooledResultSet(this, rs);
                addResultSetTrace(poolableResultSet);
                return poolableResultSet;
            }else{  //执行rest api
                JestResult result = getJestResult(sql);
                ResultSet rs = new JestResultSet(result);
                DruidPooledResultSet poolableResultSet = new DruidPooledResultSet(this, rs);
                addResultSetTrace(poolableResultSet);
                return poolableResultSet;
            }

        } catch (Throwable t) {
            throw checkException(t);
        } finally {
            conn.afterExecute();
        }
    }
    @Override
    public boolean execute() throws SQLException {
        String sql = getSql();
        if(isExecuteSql(sql)){
            executeSql(sql);
        }else{
            executeJest(sql);
        }

        return true;
    }
    @Override
    public int executeUpdate() throws SQLException {
        if(isExecuteSql(getSql())){
            throw new SQLException("executeUpdate not support in ElasticSearch sql");
        }else{
            try{
                JestResult result = getJestResult(getSql());
                return result.getResponseCode();
            }catch (Exception e){
                LOG.error("执行jest操作发生错误",e);
            }
        }
        return -1;
    }
    /**
     * 执行sql查询
     * @throws SQLException
     */
    private void executeSql(String sql)throws SQLException{
        checkOpen();
        incrementExecuteCount();
        transactionRecord(sql);
        oracleSetRowPrefetch();
        conn.beforeExecute();
        DruidPooledResultSet poolableResultSet = null;
        ResultSet rs = null;
        try {
            ObjectResult extractor = getObjectResult(true, StringUtils.join(sqlList.toArray(),""), false, false, true);
            List<String> headers = extractor.getHeaders();
            List<List<Object>> lines = extractor.getLines();
            rs = new MybatisElasticSearchResultSet(this, headers, lines);
            poolableResultSet = new DruidPooledResultSet(this, rs);
            addResultSetTrace(poolableResultSet);

            PreparedStatement stmt = this.getPreparedStatementHolder().getStatement();
            Method method = stmt.getClass().getDeclaredMethod("setResultSet",ResultSet.class);
            method.invoke(stmt,poolableResultSet);
        } catch (Throwable t) {
            throw checkException(t);
        } finally {
            conn.afterExecute();
        }
    }
    /**
     * 使用jest客户端执行rest查询
     */
    private void executeJest(String restBody)throws SQLException{
        checkOpen();
        incrementExecuteCount();
        oracleSetRowPrefetch();
        conn.beforeExecute();
        try {
            JestResult result = getJestResult(restBody);
            ResultSet rs = new JestResultSet(result);
            DruidPooledResultSet poolableResultSet = new DruidPooledResultSet(this, rs);
            addResultSetTrace(poolableResultSet);

            PreparedStatement stmt = this.getPreparedStatementHolder().getStatement();
            Method method = stmt.getClass().getDeclaredMethod("setResultSet",ResultSet.class);
            method.invoke(stmt,poolableResultSet);
        } catch (Throwable t) {
            throw checkException(t);
        } finally {
            conn.afterExecute();
        }
    }

    /**
     * 判断是否是执行sql操作
     * @param sql
     * @return true:sql操作。false：rest操作
     */
    private boolean isExecuteSql(String sql){
        int i = StringUtils.indexOf(sql,"?");
        if(i==-1){
            i=sql.length();
        }
        String head = StringUtils.substring(sql,0,i).trim();
        if(StringUtils.startsWithIgnoreCase(head,"select")){
            return true;
        }
        return false;
    }
    private ObjectResult getObjectResult(boolean flat, String query, boolean includeScore, boolean includeType, boolean includeId) throws SqlParseException, SQLFeatureNotSupportedException, Exception, CsvExtractorException {
        SearchDao searchDao = new SearchDao(client);

        //String rewriteSQL = searchDao.explain(getSql()).explain().explain();

        QueryAction queryAction = searchDao.explain(query);
        Object execution = QueryActionElasticExecutor.executeAnyAction(searchDao.getClient(), queryAction);
        return new ObjectResultsExtractor(includeScore, includeType, includeId).extractResults(execution, flat);
    }

    /**
     * 调用jest客户端执行查询
     * @param restBody
     * @return
     * @throws Exception
     */
    public JestResult getJestResult(String restBody) throws Exception{
        JestType op_type = JestUtil.checkOperateType(restBody);
        String restMapping = JestUtil.getRestMapping(restBody);
        String[] indexes = JestUtil.getIndexes(restBody);
        String[] types = JestUtil.getTypes(restBody);
        String id = JestUtil.getId(restBody);
        Map<String,Object> parameter = JestUtil.getParameters(restBody);
        JestResult result = null;
        switch (op_type){
            case ADD:
                result = StringUtils.isBlank(id)?
                        JestUtil.insert(restMapping,indexes[0],types[0],parameter):
                        JestUtil.insert(restMapping,indexes[0],types[0],id,parameter);
                break;
            case SELECT:
                result = JestUtil.query(restMapping,indexes,types,parameter);
                break;
            case GET:
                result = JestUtil.get(indexes[0],types[0],id);
                break;
            case UPDATE:
                result = JestUtil.update(restMapping,indexes[0],types[0],id.split(","));
                break;
            case DELETE:
                result = JestUtil.delete(indexes[0],types[0],id.split(","));
                break;
        }
        return result;
    }
}
