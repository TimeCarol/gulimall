package com.atguigu.gulimall.search;

import com.alibaba.fastjson.JSON;
import com.atguigu.gulimall.search.config.GulimallElasticConfig;
import lombok.*;
import lombok.experimental.Accessors;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.awt.image.ImageProducer;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@RunWith(SpringRunner.class)
@SpringBootTest
public class GulimallSearchApplicationTests {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Test
    public void contextLoads() {
        System.out.println(restHighLevelClient);
    }

    @Data
    @ToString
    @Accessors(chain = true)
    class User {
        private String username;
        private Byte age;
        private Character gender;
    }

    /**
     * 测试保存数据到ElasticSearch
     * @throws IOException
     */
    @Test
    public void indexData() throws IOException {
        IndexRequest request = new IndexRequest("users");
        request.id("1");
        User user = new User().setUsername("Alice").setAge(new Byte("18")).setGender('女');
        String jsonString = JSON.toJSONString(user);
        System.out.println(jsonString);
        request.source(jsonString, XContentType.JSON);
        //执行操作
        IndexResponse index = restHighLevelClient.index(request, GulimallElasticConfig.COMMON_OPTIONS);
        System.out.println(index);
//        request.source(
//                "username", "Alice",
//                "age", 18
//                ,"gender", "女"
//        );
    }


    @Test
    public void searchData() throws IOException {
        //创建检索请求
        SearchRequest searchRequest = new SearchRequest();
        //指定索引
        searchRequest.indices("bank");
        //指定DSL，检索条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //构造检索条件
        sourceBuilder.query(QueryBuilders.matchQuery("address", "mill"));
        //按照年龄的值分布进行聚合
        sourceBuilder.aggregation(AggregationBuilders.terms("ageAgg").field("age").size(10));
        //计算平均薪资
        sourceBuilder.aggregation(AggregationBuilders.avg("balanceAgg").field("balance"));
        searchRequest.source(sourceBuilder);



        System.out.println("检索条件：" + sourceBuilder);

        //执行检索
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, GulimallElasticConfig.COMMON_OPTIONS);
        //分析结果

        System.out.println(searchResponse);
        //获取所有查到的数据
        SearchHits hits = searchResponse.getHits();
        //
        for (SearchHit hit : hits) {
            String source = hit.getSourceAsString();
            Account account = JSON.parseObject(source, Account.class);
            System.out.println(account);
        }

        //获取这次检索到的分析信息
        Aggregations aggregations = searchResponse.getAggregations();
        Terms ageAgg = aggregations.get("ageAgg");
        for (Terms.Bucket bucket : ageAgg.getBuckets()) {
            System.out.println("年龄：" + bucket.getKeyAsString() + ", 数量：" + bucket.getDocCount());
        }
        Avg balanceAgg = aggregations.get("balanceAgg");
        System.out.println("平均薪资" + balanceAgg.getValue());

    }


    @Data
    @ToString
    @Accessors(chain = true)
    static class Account {
        private int account_number;
        private int balance;
        private String firstname;
        private String lastname;
        private int age;
        private String gender;
        private String address;
        private String employer;
        private String email;
        private String city;
        private String state;
    }
}
