package com.wjj.es.util;

/**
 * Created by wjj on 2017/6/5.
 */
public enum ElasticsearchTypes {
    /**相等，不能和null比较*/
    EQUAL,

    /**不相等，不能和null比较*/
    NOT_EQUAL,

    /**只取name存在值的数据,对应sql中 xxx!=null,value可为空*/
    EXISTS,

    /**只取name不存在值的数据,对应sql中 xxx is null,value可为空*/
    NOT_EXISTS,

    /**模糊查询，不能和null比较*/
    LIKE,

    /**大于，不能和null比较*/
    GREATER,

    /**大于等于，不能和null比较*/
    GREATER_AND_EQUAL,

    /**小于，不能和null比较*/
    SMALLER,

    /**小于等于，不能和null比较*/
    SMALLER_AND_EQUAL,

    /**范围查询，value必须传入两个参数，包含下界，不含上界*/
    RANGE,

    /**正则查询,规范见：https://www.elastic.co/guide/en/elasticsearch/reference/2.4/query-dsl-regexp-query.html */
    REGEX,

    /**通过es的_id查询，name可为空,value可传入多个_id值(String类型)查询多条记录*/
    ID,

    /**分页，name可为空，value必须传入两个参数值,页号(从1开始)和每页数量,int类型*/
    PAGINATION,

    /**排序，value值无效，可以为null。需要sortOrder参数，在没有条件查询的时候使用(select * from xxx order by xxx desc这种情况)。如果有条件查询可以在条件查询的字段上添加sortOrder属性*/
    SORT,

    /**计算记录数量，name和value可为空，使用后只返回记录数(放在Map的total_cnt属性中)不返回数据集,不指定COUNT也会在total_cnt属性中返回符合条件的记录数*/
    COUNT,

    /**聚合，name可为空，添加多个聚合时可以在value数组中放多个聚合实例*/
    AGGREGATION
    ;
}
