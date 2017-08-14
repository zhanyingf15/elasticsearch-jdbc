# elasticsearch-jdbc

该项目最先是在[elasticsearch-sql](https://github.com/NLPchina/elasticsearch-sql)的[实验特性项目](https://github.com/NLPchina/elasticsearch-sql/pull/283)的基础上修改而来,修改和补充了部分jdbc规范，使之能够和mybatis结合，sql支持度可参考elasticsearch-sql，引入了[jest](https://github.com/searchbox-io/Jest)，实现了mybatis+rest api的方式操作elasticsearch。提供了Java api的工具类`ESUtil`和rest api工具类`JestUtil`。

### 使用方式

####  maven依赖

```xml
<repositories>
    <repository>
      <id>wjj-maven-repo</id>
      <url>https://raw.github.com/zhanyingf15/maven-repo/master</url>
    </repository>
</repositories>
```
上面是在GitHub上搭建的私人仓库
```xml
<dependency>
  <groupId>com.wjj</groupId>
  <artifactId>elasticsearch-jdbc</artifactId>
  <version>2.0.1</version>
</dependency>
```

####  application.xml

```xml
<bean id="defaulteDataSource-es" class="com.alibaba.druid.pool.MybatisElasticSearchDruidDataSource" init-method="init" destroy-method="close">
		<property name="url" value="jdbc:elasticsearch://192.168.70.128:9300/" />
		<property name="initialSize" value="2" />
		<property name="minIdle" value="2" />
		<property name="maxActive" value="100" />
		<property name="maxWait" value="60000" />
		<property name="timeBetweenEvictionRunsMillis" value="60000" />
		<property name="minEvictableIdleTimeMillis" value="300000" />
		<property name="validationQuery" value="select 1 from dual" />
		<property name="testWhileIdle" value="true" />
		<property name="testOnBorrow" value="false" />
		<property name="testOnReturn" value="false" />
		<property name="poolPreparedStatements" value="false" />
		<property name="maxPoolPreparedStatementPerConnectionSize" value="20" />
		<property name="removeAbandoned" value="true" />
		<property name="removeAbandonedTimeout" value="1800" />
		<property name="logAbandoned" value="true" />
		<property name="filters" value="stat,log4j" />
		<property name="connectionProperties" value="druid.stat.slowSqlMillis=5000" />
	</bean>
	<bean id="defaultSQLSessionFactory-es" class="org.mybatis.spring.SqlSessionFactoryBean">
		<property name="dataSource" ref="defaulteDataSource-es" />
		<property name="configLocation" value="classpath:mybatis-config.xml"></property>
		<property name="mapperLocations">
			<array>
				<value>classpath*:com/**/model/es/*.xml</value>
			</array>
		</property>
	</bean>
	<bean id="defaultSQLSessionTemplate-es" class="org.mybatis.spring.SqlSessionTemplate">
		<constructor-arg index="0" ref="defaultSQLSessionFactory-es" />
	</bean>
	<bean id="jdbcTemplate" class="org.springframework.jdbc.core.JdbcTemplate">
		<property name = "dataSource" ref="defaulteDataSource-es"/>
	</bean>
```

>  elasticsearch-jdbc只能使用阿里的druid数据源

#### 添加elasticsearch.properties 配置文件到classpath

```properties
#es http address，split by "," if have multi address.eg http://ip1:port,http://ip2:port
http.url=http://192.168.70.128:9200
#max connection number
maxActive=20
```



####  在mybatis中使用sql和rest api操作

```xml
<!-- query data from oracle database -->
<select id="qryOracleList" parameterType="java.lang.Object" resultType="com.tydic.base.util.MapEntry">
  SELECT * FROM testTable 
</select>
<!--query or update data by rest api -->
<!--add-->
<insert id="jestInsert" parameterType="java.lang.Object">
	PUT ${index}/${type}/${_id}?op_type=create
	{
		"id":"${id}",
		"name":"${name}"
	}
</insert>
<!--search-->
<select id="qryTest2" parameterType="java.lang.Object" resultType="java.util.HashMap">
	POST ${index}/${type}/_search?
	{
		"query": {
			"prefix": {
				"name": "te"
			}
		}
	}
</select>

<!--query demo2，query data by _id-->
<select id="qryTest2" parameterType="java.lang.Object" resultType="java.util.HashMap">
	GET ${index}/${type}/${id}
</select>

<!--update-->
<update id="jestUpdate" parameterType="java.lang.Object">
	POST ${index}/${type}/${id}/_update?
	{
		"doc": {
			"name": "${name}"
		}
	}
</update>
<!--delete-->
<delete id="jestDeletee" parameterType="java.lang.Object">
	DELETE ${index}/${type}/${id}
</delete>
```

上面基本列出了CRUD操作，为了方便描述，xml中的内容统称为restBody，以修改为例，`POST`为协议，`index/type/id/_update`为uri，`?`和第一个`{`间的内容为参数(即添加示例中的op_type=create，可选)，`{xxx}`为restMapping，这个就是发到elasticsearch的结构体（JSON字符串）。

协议的不同，uri也会不同，参数都是可选的，协议与uri的关系如下：

- PUT：insert操作。
  uri必须含有index/type[/id]，id可选，默认使用es的自增长id，restMapping不能为空
- POST：select或update操作。
  如果是update，uri必须满足`index/type/id/_update`格式。id可以传入多个批量更新(如更新多个文档的state)，使用英文`,`分隔
  如果是select，index/type都是可选，也可以传入多个index和type，用英文`,`分隔，如：index1,index2/type1,type2/_search，不能有id，必须带_search，所以select操作只能是下面几种uri
  - `_search`：不指定index和type
  - `index/_search`：只指定index
  - `index/type/_search`：同时指定index和type
- DELETE：delete操作。
  uri必须满足`index/type/id`这种格式。id可以传入多个批量删除，使用英文`,`分隔，不能根据条件删除，所以传入restMapping是无效的
- GET：通过文档id获取。
  只能通过单个文档id获取，，uri必须满足`index/type/id`格式，只能指定一个id，如果需要同时通过多个id查询，使用[idsQuery](https://www.elastic.co/guide/en/elasticsearch/reference/2.4/query-dsl-ids-query.html)，传入restMapping无效。

> 协议不区分大小写，参数可选。但是无论是否有参数，只要有restMapping，`?`必须带上

**注意**
所有的增删改查都可以包裹在`<select>`中，如果是包裹在`<select>`中，都可以在java中通过以下方式获取结果

```java
Map result = session.selectOne("xxx",param);
//如果是增、删、改操作。
JestResult result = JestUtil.getExecuteResult(result);
//如果是查询
SearchResult result = JestUtil.getQueryResult(result);
```

如果增删改是包裹在对应的`<insert>`、`<update>`和`<delete>`中，通过session.insert、session.update、session.delete操作返回的整形数字不是关系型数据库中影响的行数，而是http状态码，2xx系列状态码表示操作成功，如果操作失败，并不会返回具体的错误信息，程序也不会报错。如果需要具体的操作信息，只能使用`<select>`包裹，返回`JestResult`对象，再调用相应的方法获取具体信息。


>  操作elasticsearch时，在mybatis的xml中只能使用`${}`而不是`#{}`，因为elasticsearch并不是数据库，并没有数据库元数据，后续考虑会构建一个elasticsearch的元数据对象。


#### 测试

service业务层

```java
@Resource(name = "defaultSQLSessionTemplate-es")
protected SqlSessionTemplate sessionES;
public Map<String,Object> qryJest(Map param){
        Map result = new HashMap<>();
        JestResult jestResult = JestUtil.getQueryResult(sessionES.selectOne("es.qryTest2",param));
        result.put("data_list",jestResult.getSourceAsObjectList(Map.class));
        return result;
}
```

main方法

```java
ClassPathXmlApplicationContext ac = new ClassPathXmlApplicationContext("classpath:applicationContext.xml");
ac.start();
SqlSessionTemplate esSession = SpringBeanUtil.getInstance().getBean("defaultSQLSessionTemplate-es",SqlSessionTemplate.class);
ESTestServiceImpl esTestService = SpringBeanUtil.getInstance().getBean("esTestService",ESTestServiceImpl.class);
Map param = new HashMap();
param.put("index","radiott");
param.put("type","artiststt");
param.put("name","te");
Map result = esTestService.qryJest(param);
```

不依赖spring和mybatis的使用方式
```java
/**
 * Created by wjj on 2017/8/7.
 * 不依赖spring和mybatis的使用方式
 */
public class Test {
    public static DruidDataSource dataSource = getDataSource();
    public static DruidDataSource getDataSource(){
        DruidDataSource dataSource = null;
        try{
            Map properties = new HashMap<>();
            properties.put("url","jdbc:elasticsearch://192.168.70.128:9300");
            properties.put("http.url","http://192.168.70.128:9200");
            properties.put("maxActive","20");
            dataSource = (DruidDataSource) MybatisElasticSearchDruidDataSourceFactory.createDataSource(properties);
        }catch (Exception e){
            e.printStackTrace();
        }
        return dataSource;
    }
    public static void testJDBC() throws Exception{
        String sql = "select * from bank/account where firstname='Effie'";
        Connection connection = dataSource.getConnection();
        PreparedStatement ps = connection.prepareStatement(sql);
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            System.out.println(resultSet.getString("firstname") + "," + resultSet.getInt("age"));
        }
        connection.close();
        dataSource.close();
    }

    /**
     * 如果单独使用rest api时不需要创建连接，也可以不创建数据源(需要手动调用initJestClient()初始化客户端)
     * @throws Exception
     */
    public static void testRest() throws Exception{
//        JestUtil.initJestClient(properties);//该过程在创建数据源时已执行
        String restStr = "{ " +
                         " \"query\":{" +
                         "   \"term\":{ " +
                         "       \"firstname\":\"effie\"" +
                         "     }" +
                         "   }" +
                         "}";
        SearchResult result = JestUtil.query(restStr,new String[]{"bank"},new String[]{"account"});
        List<SearchResult.Hit<Map,Void>> list = result.getHits(Map.class);
        for(SearchResult.Hit<Map,Void> hit:list){
            Iterator<Map.Entry<String,Object>> i = hit.source.entrySet().iterator();
            while(i.hasNext()){
                Map.Entry<String,Object> entry = i.next();
                System.out.println(entry.getKey()+":"+entry.getValue());
            }

        }
    }
    public static void main(String[] args) throws Exception{
        testJDBC();
        testRest();
    }
}
```
