package com.wjj.jdbc.jest;

import com.wjj.jdbc.util.PropertiesUtil;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.*;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by wjj on 2017/6/21.
 */
public class JestUtil {
    private JestUtil(){}

    private static class ClientHolder{
        public static JestClient jestClient = createJestClient();
        private static JestClient createJestClient(){
            String jdbcUrl = PropertiesUtil.getValue("http.url");//从配置文件中获取配置信息
            if(StringUtils.isBlank(jdbcUrl)){
                throw new RuntimeException("缺少http.url配置项，创建JestClient失败");
            }
            JestClientFactory factory = new JestClientFactory();
            String[] hostAndPortArray = jdbcUrl.split(",");
            String maxActive = PropertiesUtil.getValue("maxActive","20");
            factory.setHttpClientConfig(new HttpClientConfig
                    .Builder(Arrays.asList(hostAndPortArray))
                    .multiThreaded(true)
                    //Per default this implementation will create no more than 2 concurrent connections per given route
//                .defaultMaxTotalConnectionPerRoute(5)
                    // and no more 20 connections in total
                    .maxTotalConnection(Integer.parseInt(maxActive))
                    .build());
            return factory.getObject();
        }
    }

    public static JestClient getJestClient(){
        return ClientHolder.jestClient;
    }

    /**
     * 获取增删改的操作结果
     * @param map
     * @return
     */
    public static JestResult getExecuteResult(Map map){
        return (JestResult) map.get(JestMetaData.COLUMN_LABEL);
    }

    /**
     * 获取查询的操作结果
     * @param map
     * @return
     */
    public static SearchResult getQueryResult(Map map){
        return (SearchResult) map.get(JestMetaData.COLUMN_LABEL);
    }
    /**
     * 插入额外参数
     * @param builder
     * @param parameter
     * @throws Exception
     */
    private static void setParameters(Object builder,Map parameter) throws Exception{
        if(parameter==null||parameter.isEmpty()){
            return;
        }
        Method method = builder.getClass().getMethod("setParameter",String.class,Object.class);
        Iterator<Map.Entry<String,Object>> i = parameter.entrySet().iterator();
        while (i.hasNext()){
            Map.Entry<String,Object> e = i.next();
            method.invoke(builder,e.getKey(),e.getValue());
        }

    }
    /**
     * 插入数据，文档_id由elasticsearch自动生成
     * @param jsonStr 文档json字符串
     * @param index  索引
     * @param type  类型
     * @return
     * @throws IOException
     */
    public static JestResult insert(String jsonStr,String index,String type)throws Exception{
        return insert(jsonStr,index,type,new HashMap());
    }
    /**
     * 插入数据，文档_id由elasticsearch自动生成
     * @param jsonStr 文档json字符串
     * @param index  索引
     * @param type  类型
     * @param parameter  额外参数
     * @return
     * @throws IOException
     */
    public static JestResult insert(String jsonStr,String index,String type,Map<String,Object> parameter)throws Exception{
        Index.Builder builder = new Index.Builder(jsonStr).index(index).type(type).refresh(true);
        setParameters(builder,parameter);
        Index jIndex = builder.build();
        return getJestClient().execute(jIndex);
    }
    /**
     * 插入数据，使用指定的id作为文档_id
     * @param jsonStr 文档json字符串
     * @param index 索引
     * @param type 类型
     * @param id
     * @return
     * @throws IOException
     */
    public static JestResult insert(String jsonStr,String index,String type,String id)throws Exception{
        return insert(jsonStr,index,type,id,null);
    }
    /**
     * 插入数据，使用指定的id作为文档_id
     * @param jsonStr 文档json字符串
     * @param index 索引
     * @param type 类型
     * @param id
     * @param parameter  额外参数
     * @return
     * @throws IOException
     */
    public static JestResult insert(String jsonStr,String index,String type,String id,Map<String,Object> parameter)throws Exception{
        Index.Builder builder = new Index.Builder(jsonStr).index(index).type(type).id(id).refresh(true);
        setParameters(builder,parameter);
        Index jIndex = builder.build();
        return getJestClient().execute(jIndex);
    }

    /**
     * 查询
     * @param jsonStr json字符串
     * @param indexes 索引
     * @param types 类型
     * @return
     * @throws IOException
     */
    public static SearchResult query(String jsonStr,String[]indexes,String[] types)throws Exception{
        return query(jsonStr,indexes,types,null);
    }
    /**
     * 查询
     * @param jsonStr json字符串
     * @param indexes 索引
     * @param types 类型
     * @param parameter  额外参数
     * @return
     * @throws IOException
     */
    public static SearchResult query(String jsonStr,String[]indexes,String[] types,Map<String,Object> parameter)throws Exception{
        Search.Builder searchBuilder = new Search.Builder(jsonStr);
        if(indexes!=null&&indexes.length>0){
            searchBuilder.addIndex(Arrays.asList(indexes));
        }
        if(types!=null&&types.length>0){
            searchBuilder.addType(Arrays.asList(types));
        }
        setParameters(searchBuilder,parameter);
        SearchResult result = getJestClient().execute(searchBuilder.build());
        return result;
    }


    /**
     * 根据文档id获取文档
     * @param index 索引
     * @param type 类型
     * @param id 文档id
     * @return
     * @throws IOException
     */
    public static JestResult get(String index,String type,String id)throws IOException{
        Get get = new Get.Builder(index,id).type(type).build();
        return getJestClient().execute(get);
    }

    /**
     * 更新
     * @param jsonStr 文档json字符串
     * @param index 索引
     * @param type  类型
     * @param id 文档id
     * @return
     * @throws IOException
     */
    public static JestResult update(String jsonStr,String index,String type,String id)throws IOException{
        Update update = new Update.Builder(jsonStr).index(index).type(type).id(id).refresh(true).build();
        JestResult result = getJestClient().execute(update);
        return result;
    }
    /**
     * 批量更新
     * @param jsonStr 文档json字符串
     * @param index 索引
     * @param type  类型
     * @param ids 文档id
     * @return
     * @throws IOException
     */
    public static JestResult update(String jsonStr,String index,String type,String ...ids)throws IOException{
        Bulk.Builder bulkBuilder = new Bulk.Builder().defaultIndex(index).defaultType(type).refresh(true);
        for(String id:ids){
            bulkBuilder.addAction(new Update.Builder(jsonStr).index(index).type(type).id(id).build());
        }
        JestResult result = getJestClient().execute(bulkBuilder.build());
        return result;
    }

    /**
     * 删除
     * @param index 索引
     * @param type 类型
     * @param id 文档id
     * @return
     * @throws IOException
     */
    public static JestResult delete(String index,String type,String id) throws IOException{
        Delete delete = new Delete.Builder(id).index(index).type(type).refresh(true).build();
        return getJestClient().execute(delete);
    }

    /**
     * 批量删除
     * @param index 索引
     * @param type 类型
     * @param ids 文档id
     * @return
     * @throws IOException
     */
    public static JestResult delete(String index,String type,String ...ids) throws IOException{
        Bulk.Builder bulkBuilder = new Bulk.Builder().defaultIndex(index).defaultType(type).refresh(true);
        for(String id:ids){
            bulkBuilder.addAction(new Delete.Builder(id).index(index).type(type).build());
        }
        return getJestClient().execute(bulkBuilder.build());
    }

    /**
     * 检查rest操作类型
     * @param restBody
     * @return JestType
     */
    public static JestType checkOperateType(String restBody){
        if(StringUtils.isBlank(restBody)){
            throw new RuntimeException("rest body 不能为空");
        }
        int i = StringUtils.indexOf(restBody,"?");
        if(i==-1){
            i=restBody.length();
        }
        String head = StringUtils.substring(restBody,0,i).trim();
        if(StringUtils.startsWithIgnoreCase(head,"put")){
            return JestType.ADD;
        }else if(StringUtils.startsWithIgnoreCase(head,"post")){
            if(head.contains("_search")){
                return JestType.SELECT;
            }else if(head.contains("_update")){
                return JestType.UPDATE;
            }else{
                return JestType.ADD;
            }
        }else if(StringUtils.startsWithIgnoreCase(head,"get")){
            return JestType.GET;
        }else if(StringUtils.startsWithIgnoreCase(head,"delete")){
            return JestType.DELETE;
        }else{
            throw new RuntimeException("不合法的rest body 格式，只能以put,post,get,delete开头");
        }
    }

    /**
     * 获取结构体
     * @param restBody
     * @return
     */
    public static String getRestMapping(String restBody){
        int i = StringUtils.indexOf(restBody,"{");
        return StringUtils.substring(restBody,i).trim();
    }

    /**
     * 获取参数
     * @param restBody
     * @return
     */
    public static Map<String,Object> getParameters(String restBody){
        int i1 = StringUtils.indexOf(restBody,"?");
        int i2 = StringUtils.indexOf(restBody,"{");
        if(i1==-1)i1=0;
        if(i2==-1)i2=restBody.length();
        String pStr = StringUtils.substring(restBody,i1+1,i2).trim();
        Map<String,Object> map = null;
        if(StringUtils.isNoneBlank(pStr)){
            map = new HashMap<>();
            String[] pairs = pStr.split("&");
            for(String pair:pairs){
                String[] ps = pair.split("=",2);
                String key = ps[0].trim();
                String value = ps.length==2?ps[1].trim():"";
                map.put(key,value);
            }
        }
        return map;
    }
    /**
     * 获取URI
     * @param restBody
     * @return
     */
    private static String getUri(String restBody){
        int i = StringUtils.indexOf(restBody,"?");
        if(i==-1){
            i=restBody.length();
        }
        String head = StringUtils.substring(restBody,0,i).trim();
        String regx = "(put|post|get|delete)\\s+(\\S+)";
        Pattern pattern = Pattern.compile(regx,Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(head);
        if(matcher.find()){
            return matcher.group(2).trim();
        }else{
            return "";
        }
    }

    /**
     * 获取索引
     * @param restBody
     * @return
     */
    public static String[] getIndexes(String restBody){
        String uri = getUri(restBody);
        if(StringUtils.isBlank(uri)||uri.equals("_search")){
            return null;
        }
        String [] strs = uri.split("/");
        return strs[0].trim().split(",");

    }

    /**
     * 获取类型
     * @param restBody
     * @return
     */
    public static String[] getTypes(String restBody){
        String uri = getUri(restBody);
        String [] strs = uri.split("/");
        if(strs.length==2&&StringUtils.isNotBlank(strs[1])&&strs[1].equals("_search")){
            return null;
        }
        if(strs.length>2){
            return strs[1].trim().split(",");
        }
        return null;
    }

    /**
     * 获取文档id
     * @param restBody
     * @return
     */
    public static String getId(String restBody){
        String uri = getUri(restBody);
        String [] strs = uri.split("/");
        JestType op_type = checkOperateType(restBody);
        if(op_type==JestType.SELECT){
            return null;
        }else if(op_type==JestType.UPDATE&&uri.contains("_update")){//update必须满足 index/type/id/_update格式
            if(strs.length!=4){
                throw new RuntimeException("错误的rest body uri格式");
            }else{
                return strs[2].trim();
            }
        }else if(op_type==JestType.ADD&&strs.length==2){ //put添加文档，如果不指定id返回null
            return null;
        }else{ //delete，get必须满足 index/type/id格式，put可以指定id替代es生成id。
            if(strs.length!=3){
                throw new RuntimeException("错误的rest body uri格式");
            }else {
                return strs[2].trim();
            }
        }
    }
}
