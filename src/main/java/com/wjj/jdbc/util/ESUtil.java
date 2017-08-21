package com.wjj.jdbc.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

/**
 * Created by wjj on 2017/6/2.
 */
public class ESUtil {
    public static Logger logger = LoggerFactory.getLogger(ESUtil.class);
    private static Client client;
    private static Client createClient(String jdbcUrl){
        if(StringUtils.isBlank(jdbcUrl)){
            jdbcUrl = PropertiesUtil.getValue("java.url");
        }
        TransportClient transportClient = null;
        Settings settings = Settings.builder().put("client.transport.ignore_cluster_name", true).build();
        try {
            transportClient = TransportClient.builder().settings(settings).build();

            String hostAndPortArrayStr = jdbcUrl.split("/")[2];
            String[] hostAndPortArray = hostAndPortArrayStr.split(",");

            for (String hostAndPort : hostAndPortArray) {
                String host = hostAndPort.split(":")[0];
                String port = hostAndPort.split(":")[1];
                transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), Integer.parseInt(port)));
            }
        } catch (UnknownHostException e) {
            logger.error("",e);
        }
        return transportClient;
    }
    public static Client getClient(){
        return getClient(null);
    }
    public static Client getClient(String jdbcUrl){
        if(client==null){
            client = createClient(jdbcUrl);
        }
        return client;
    }
    public static Client getNewClient(String jdbcUrl){
        return createClient(jdbcUrl);
    }

    /**
     * 插入数据
     * @param index 索引
     * @param type 类型
     * @param params
     * @return
     */
    public static IndexResponse insert(String index,String type,Map params){
        return getClient().prepareIndex(index,type).setRefresh(true).setSource(params).get();
    }
    /**
     * 插入数据,使用指定id作为文档id
     * @param index 索引
     * @param type 类型
     * @param id 文档id
     * @param params
     * @return
     */
    public static IndexResponse insert(String index,String type,String id,Map params){
        return getClient().prepareIndex(index,type).setId(id).setRefresh(true).setSource(params).get();
    }
    /**
     * 批量插入数据
     * @param index 索引
     * @param type 类型
     * @param params
     * @return BulkResponse
     */
    public static BulkResponse insert(String index,String type,Map ...params){
        Client client = getClient();
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        for(Map param:params){
            IndexRequestBuilder indexBuilder = client.prepareIndex(index,type).setSource(param);
            bulkRequestBuilder.add(indexBuilder);
        }
        return bulkRequestBuilder.setRefresh(true).get();
    }

    /**
     * 删除数据
     * @param index 索引
     * @param type 类型
     * @param id 记录id
     * @return
     */
    public static DeleteResponse delete(String index,String type,String id){
        return getClient().prepareDelete(index,type,id).setRefresh(true).get();
    }

    /**
     * 批量删除数据
     * @param index 索引
     * @param type 类型
     * @param ids 文档id
     * @return
     */
    public static BulkResponse delete(String index, String type, String ...ids){
        Client client = getClient();
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        for(String id:ids){
            DeleteRequestBuilder delBuilder = client.prepareDelete(index,type,id);
            bulkRequestBuilder.add(delBuilder);
        }
        return bulkRequestBuilder.setRefresh(true).get();
    }

    /**
     * 更新，如果id不存在，抛出异常
     * @param index 索引
     * @param type 类型
     * @param id 文档id
     * @param params
     * @return
     */
    public static UpdateResponse update(String index,String type,String id,Map params){
        return update(index,type,id,params,false);
    }
    /**
     * 批量更新,用作批量更新一组文档的某个字段，如更改state等等
     * @param index 索引
     * @param type 类型
     * @param ids 文档id数组
     * @param params
     * @return
     */
    public static BulkResponse update(String index,String type,String[] ids,Map params){
        Client client = getClient();
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        for(String id:ids){
            UpdateRequestBuilder update = client.prepareUpdate(index,type,id);
            bulkRequestBuilder.add(update);
        }
        return bulkRequestBuilder.setRefresh(true).get();
    }
    /**
     * 更新
     * @param index 索引
     * @param type 类型
     * @param params
     * @param upsert 是否更新插入，如果为true，在指定的_id不存在时执行插入操作,否则抛出异常
     * @return
     */
    public static UpdateResponse update(String index,String type,String id,Map params,boolean upsert){
        return getClient().prepareUpdate(index,type,id)
                .setDocAsUpsert(upsert)
                .setRefresh(true).setDoc(params).get();
    }

    /**
     *通过文档id查询
     * @param index 索引
     * @param type 类型，可以为null
     * @param id 文档id
     * @return
     */
    public static GetResponse get(String index, String type, String id){
        return getClient().prepareGet(index,type,id).get();
    }
    /**
     *通过文档id查询,批量操作
     * @param index 索引
     * @param type 类型，可以为null
     * @param ids 文档id
     * @return
     */
    public static Map get(String index, String type, String ...ids){
        return get(new String[]{index},StringUtils.isBlank(type)?null:new String[]{type},ids);
    }

    /**
     * 通过文档id查询，可跨多个索引和类型
     * @param index 索引
     * @param types 类型,可为null
     * @param ids
     * @return
     */
    public static Map get(String[] index,String[] types,String ...ids){
        Client client = getClient();
        SearchRequestBuilder builder = client.prepareSearch(index).setSearchType(SearchType.DFS_QUERY_AND_FETCH);
        if(types!=null&&types.length>0){
            builder.setTypes(types);
        }
        builder.setQuery(QueryBuilders.idsQuery(ids));
        return handleSearchResponse(builder.execute().actionGet());
    }
    /**
     * 查询，支持同mysql类似的分页功能，只返回查询结果
     * @param index 索引
     * @param types 类型
     * @param params 查询参数
     * @return Map
     */
    public static Map selectList(String[] index,String[] types,List<ElasticsearchField> params){
        return handleSearchResponse(selectListResponse(index,types,params));
    }
    /**
     * 查询，支持同mysql类似的分页功能
     * @param index 索引
     * @param types 类型
     * @param params 查询参数
     * @return Map
     */
    public static SearchResponse selectListResponse(String[] index,String[] types,List<ElasticsearchField> params){
        Client client = getClient();
        SearchRequestBuilder builder = client.prepareSearch(index).setTypes(types).setSearchType(SearchType.DFS_QUERY_AND_FETCH);
        builder.setVersion(true);
        buildQueryFiled(builder,params);
        SearchResponse response = builder.execute().actionGet();
        return response;
    }
    /**
     * 使用scroll分页的方式查询，只返回查询结果，ElasticsearchTypes.COUNT和ElasticsearchTypes.PAGINATION无效
     * <p>https://www.elastic.co/guide/en/elasticsearch/reference/2.4/search-request-scroll.html</p>
     * @param index 索引
     * @param types 类型
     * @param params 查询参数
     * @param keepAlive 快照时间
     * @param scrollId 上次分页标识id，初始查询时为null
     * @param size 每个分片返回查询的数量，最终返回的数量是:[0~分片数*size]
     * @return Map
     */
    public static Map selectList(String[] index, String[] types, List<ElasticsearchField> params, TimeValue keepAlive,String scrollId,int size){
        return handleSearchResponse(selectListResponse(index,types,params,keepAlive,scrollId,size));
    }
    /**
     * 使用scroll分页的方式查询，ElasticsearchTypes.COUNT和ElasticsearchTypes.PAGINATION无效
     * <p>https://www.elastic.co/guide/en/elasticsearch/reference/2.4/search-request-scroll.html</p>
     * @param index 索引
     * @param types 类型
     * @param params 查询参数
     * @param keepAlive 快照时间
     * @param scrollId 上次分页标识id，初始查询时为null
     * @param size 每个分片返回查询的数量，最终返回的数量是:[0~分片数*size]
     * @return
     */
    public static SearchResponse selectListResponse(String[] index, String[] types, List<ElasticsearchField> params, TimeValue keepAlive,String scrollId,int size){
        if(keepAlive==null){
            throw new RuntimeException("keepAlive 不能为空");
        }
        if(size<=0)throw new RuntimeException("size必须是一个正整数");
        Client client = getClient();
        if(StringUtils.isBlank(scrollId)){
            SearchRequestBuilder builder = client.prepareSearch(index).setTypes(types).setScroll(keepAlive).setSearchType(SearchType.DFS_QUERY_AND_FETCH);
            builder.setVersion(true);
            buildQueryFiled(builder,params);
            builder.setSize(size);
            SearchResponse response = builder.execute().actionGet();
            return response;
        }else{
            SearchResponse resultResponse = client.prepareSearchScroll(scrollId).setScroll(keepAlive).execute().actionGet();
            return resultResponse;
        }
    }

    public static Map handleSearchResponse(SearchResponse response){
        SearchHits hits = response.getHits();
        Iterator<SearchHit> i = hits.iterator();
        Map result = new HashMap();
        List<Map> dataList = new ArrayList<>();
        result.put("data_list",dataList);
        while(i.hasNext()){
            SearchHit hit = i.next();
            Map item = hit.getSource();
            item.put("_id",hit.getId());
            dataList.add(item);
        }
        result.put("total_cnt",hits.getTotalHits());
        String scrollId = response.getScrollId();
        if(StringUtils.isNotBlank(scrollId)){
            result.put("scroll_id",scrollId);
        }
        return result;
    }
    public static void buildQueryFiled(SearchRequestBuilder builder,List<ElasticsearchField> paramList){
        if(paramList==null||paramList.size()==0){
            return;
        }
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        for(ElasticsearchField field:paramList){
            QueryBuilder qb = null;
            Object[] value = field.getFieldValue();
            String name = field.getFieldName();
            switch (field.getType()){
                case EQUAL:
                    qb = QueryBuilders.matchQuery(name,value[0]);
                    break;
                case NOT_EQUAL:
                    qb = QueryBuilders.boolQuery().mustNot(QueryBuilders.matchQuery(name,value[0]));
                    break;
                case EXISTS:
                    qb = QueryBuilders.existsQuery(field.getFieldName());
                    break;
                case NOT_EXISTS:
                    qb = QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(field.getFieldName()));
                    break;
                case LIKE:
                    qb = QueryBuilders.wildcardQuery(name,"*"+value[0].toString().toLowerCase()+"*");
//                    qb = QueryBuilders.fuzzyQuery(name,value[0].toString().toLowerCase());
                    break;
                case GREATER:
                    qb = QueryBuilders.rangeQuery(name).gt(value[0]);
                    break;
                case GREATER_AND_EQUAL:
                    qb = QueryBuilders.rangeQuery(name).gte(value[0]);
                    break;
                case SMALLER:
                    qb = QueryBuilders.rangeQuery(name).lt(value[0]);
                    break;
                case SMALLER_AND_EQUAL:
                    qb = QueryBuilders.rangeQuery(name).lte(value[0]);
                    break;
                case RANGE:
                    if(value.length<2){
                        throw new RuntimeException("range查询value参数长度必须是2");
                    }else if(value[0]==null||value[1]==null){
                        throw new RuntimeException("range查询value参数值不能为null");
                    }
                    qb = QueryBuilders.rangeQuery(name).from(value[0]).to(value[1]).includeLower(true).includeUpper(false);
                    break;
                case REGEX:
                    qb = QueryBuilders.regexpQuery(name,((String)value[0]).toLowerCase());
                    break;
                case ID:
                    qb = QueryBuilders.idsQuery().addIds((String[]) value);
                    break;
                case PAGINATION://使用scroll分页时，传统分页方式无效
                    if(!isScrollRequest(builder)){
                        if(value.length<2||!NumberUtils.isNumber(value[0].toString())||!NumberUtils.isNumber(value[1].toString())){
                            throw new RuntimeException("分页查询value参数长度必须是2并且都为数字");
                        }
                        int page_req = NumberUtils.toInt(value[0].toString());
                        if(page_req<=0)page_req=1;
                        int page_size = NumberUtils.toInt(value[1].toString());
                        if(page_size<=0)page_size=Integer.MAX_VALUE;
                        builder.setFrom((page_req-1)*page_size).setSize(page_size).setExplain(true);
                    }
                    break;
                case SORT:
                    break;
                case COUNT://使用scroll分页时，传统计算总数方式无效
                    if(!isScrollRequest(builder)) {
                        builder.setSearchType(SearchType.COUNT).setSize(0);
                    }
                    break;
                case AGGREGATION:
                    for(int i=0;i<value.length;i++){
                        Object v = value[i];
                        if(v instanceof AbstractAggregationBuilder){
                            builder.addAggregation((AbstractAggregationBuilder)v);
                        }else{
                            throw new RuntimeException("聚合实例类型错误，不是AbstractAggregationBuilder类型");
                        }
                    }
                    break;
                default:
                    qb = QueryBuilders.matchAllQuery();
                    break;
            }
            if(qb!=null){
                if(field.getConnectSymbol()== QueryStringQueryBuilder.Operator.AND){
                    queryBuilder.must(qb);
                }else if(field.getConnectSymbol()== QueryStringQueryBuilder.Operator.OR){
                    queryBuilder.should(qb);
                }
            }
            if(field.getSortOrder()!=null){
                builder.addSort(field.getFieldName(),field.getSortOrder());
            }
        }
        builder.setQuery(queryBuilder);
    }
    private static boolean isScrollRequest(SearchRequestBuilder builder){
        if(builder==null)throw new RuntimeException("builder 不能为空");
        return builder.request().scroll()!=null;
    }
}
