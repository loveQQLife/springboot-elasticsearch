package com.kraken;

import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author kraken
 * @date 2021/6/14 17:10
 */
@Component
public class ElasticSearchCurdProvider {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

}
