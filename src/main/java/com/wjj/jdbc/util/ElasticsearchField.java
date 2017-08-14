package com.wjj.jdbc.util;

import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.sort.SortOrder;

/**
 * Created by wjj on 2017/6/5.
 */
public class ElasticsearchField {
    private ElasticsearchTypes type = ElasticsearchTypes.EQUAL;
    private QueryStringQueryBuilder.Operator connectSymbol = QueryStringQueryBuilder.Operator.AND;
    private String fieldName;
    private Object[] fieldValue;

    private SortOrder sortOrder = null;

    public ElasticsearchField(String fieldName,Object[] fieldValue){
        this.fieldName = fieldName;
        this.fieldValue = fieldValue;
    }
    public ElasticsearchField(String fieldName,Object[] fieldValue,ElasticsearchTypes type){
        this.type = type;
        this.fieldName = fieldName;
        this.fieldValue = fieldValue;
    }
    public ElasticsearchField(String fieldName,Object[] fieldValue,ElasticsearchTypes type,QueryStringQueryBuilder.Operator connectSymbol){
        this.type = type;
        this.fieldName = fieldName;
        this.fieldValue = fieldValue;
        this.connectSymbol = connectSymbol;
    }
    public ElasticsearchField(String fieldName,Object[] fieldValue,ElasticsearchTypes type,SortOrder sortOrder){
        this.type = type;
        this.fieldName = fieldName;
        this.fieldValue = fieldValue;
        this.sortOrder = sortOrder;
    }
    public ElasticsearchField(String fieldName,Object[] fieldValue,ElasticsearchTypes type,QueryStringQueryBuilder.Operator connectSymbol,SortOrder sortOrder){
        this.type = type;
        this.fieldName = fieldName;
        this.fieldValue = fieldValue;
        this.sortOrder = sortOrder;
        this.connectSymbol = connectSymbol;
    }

    public ElasticsearchTypes getType() {
        return type;
    }

    public void setType(ElasticsearchTypes type) {
        this.type = type;
    }

    public SortOrder getSortOrder() {
        return sortOrder;
    }

    public void setSortOrder(SortOrder sortOrder) {
        this.sortOrder = sortOrder;
    }

    public Object[] getFieldValue() {
        return fieldValue;
    }

    public void setFieldValue(Object[] fieldValue) {
        this.fieldValue = fieldValue;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public QueryStringQueryBuilder.Operator getConnectSymbol() {
        return connectSymbol;
    }

    public void setConnectSymbol(QueryStringQueryBuilder.Operator connectSymbol) {
        this.connectSymbol = connectSymbol;
    }
}
