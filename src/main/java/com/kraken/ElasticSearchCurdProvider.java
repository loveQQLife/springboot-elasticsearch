package com.kraken;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author kraken
 * @date 2021/6/14 17:10
 */
@Slf4j
public class ElasticSearchCurdProvider<T> {

    private RestHighLevelClient restHighLevelClient;

    private ElasticSearchCurdProvider(){

    }

    protected ElasticSearchCurdProvider(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    /**
     * 创建索引
     *
     * @param indexName   索引名称
     * @param shardsNum
     * @param replicasNum
     * @return
     */
    protected boolean createIndexMap(String indexName, int shardsNum, int replicasNum) {
        try {
            CreateIndexRequest request = new CreateIndexRequest(indexName);
            request.settings(Settings.builder()
                    .put("index.number_of_shards", shardsNum)
                    .put("index.number_of_replicas", replicasNum)
            );

            Map<String, Object> properties = new HashMap<>();
            Map<String, Object> mapping = new HashMap<>();
            mapping.put("properties", properties);
            request.mapping(mapping);

            CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
            if (createIndexResponse.isAcknowledged() && createIndexResponse.isShardsAcknowledged()) {
                log.info("create index success indexName : {}", indexName);
                return true;
            }
        } catch (Exception e) {
            log.error("create index  fail : {}", e);
        }
        return false;
    }

    /**
     * 创建索引
     *
     * @param indexName   索引名称
     * @param shardsNum
     * @param replicasNum
     * @return
     */
    protected boolean createIndexXContent(String indexName, int shardsNum, int replicasNum) {
        try {
            CreateIndexRequest request = new CreateIndexRequest(indexName);
            request.settings(Settings.builder()
                    .put("index.number_of_shards", shardsNum)
                    .put("index.number_of_replicas", replicasNum)
            );

            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                builder.startObject("properties");
                {
                }
                builder.endObject();
            }
            builder.endObject();
            request.mapping(builder);

            CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
            if (createIndexResponse.isAcknowledged() && createIndexResponse.isShardsAcknowledged()) {
                log.info("create index success indexName : {}", indexName);
                return true;
            }
        } catch (Exception e) {
            log.error("create index  fail : {}", e);
        }
        return false;
    }

    /**
     * 删除索引
     *
     * @param indexName
     * @return
     */
    protected boolean deleteIndex(String indexName) {
        try {
            DeleteIndexRequest request = new DeleteIndexRequest(indexName);
            AcknowledgedResponse deleteIndexResponse = restHighLevelClient.indices().delete(request, RequestOptions.DEFAULT);
            if (deleteIndexResponse.isAcknowledged()) {
                log.info("delete index: {} success ", indexName);
                return true;
            }
            System.out.println(deleteIndexResponse.isAcknowledged());
        } catch (Exception e) {
            log.error("delete index fail : {}", e);
        }
        return false;
    }

    /**
     * 判断索引是否存在
     *
     * @param indexName
     * @return
     */
    protected boolean existsIndex(String indexName) {
        try {
            GetIndexRequest request = new GetIndexRequest(indexName);
            boolean exists = restHighLevelClient.indices().exists(request, RequestOptions.DEFAULT);
            if (exists) {
                log.info(" index: {} exists ", indexName);
                return true;
            }
            log.info(" index: {} not exists ", indexName);
        } catch (Exception e) {
            log.error("judge exists index fail : {}", e);
        }
        return false;
    }

    /**
     * 获取索引的mapping
     *
     * @param indexName
     * @return
     */
    protected Map<String, Object> getIndexMapping(String indexName) {
        try {
            GetMappingsRequest request = new GetMappingsRequest();
            request.indices(indexName);
            GetMappingsResponse getMappingResponse = restHighLevelClient.indices().getMapping(request, RequestOptions.DEFAULT);
            Map<String, MappingMetaData> allMappings = getMappingResponse.mappings();
            MappingMetaData indexMapping = allMappings.get(indexName);
            return indexMapping.sourceAsMap();
        } catch (Exception e) {
            log.error("getIndexMapping fail : {}", e);
        }
        return null;
    }

    /**
     * 获取索引的Settings信息
     *
     * @param indexName
     * @return
     */
    protected JSONObject getIndexSettings(String indexName) throws IOException {

        try {
            JSONObject data = new JSONObject(2);
            GetSettingsRequest request = new GetSettingsRequest().indices(indexName);
            GetSettingsResponse getSettingsResponse = restHighLevelClient.indices().getSettings(request, RequestOptions.DEFAULT);
            String numberOfShardsString = getSettingsResponse.getSetting(indexName, "index.number_of_shards");
            String numberOfReplicasString = getSettingsResponse.getSetting(indexName, "index.number_of_replicas");
            data.put(numberOfShardsString, numberOfShardsString);
            data.put(numberOfReplicasString, numberOfReplicasString);
            return data;
        } catch (Exception e) {
            log.error("getIndexSettings fail : {}", e);
        }
        return null;
    }

    /**
     * 添加/更新文档
     *
     * @param indexName
     * @param documentId
     * @param t
     * @return
     */
    protected boolean insertDocument(String indexName, String documentId, T t) {

        try {
            IndexRequest indexRequest = new IndexRequest(indexName)
                    .id(documentId)
                    .source(JSONObject.toJSONString(t));
            IndexResponse indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
            if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
                log.info("create Document success id : {},version:  {}", indexResponse.getId(), indexResponse.getVersion());
                return true;
            }
            if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                log.info("updated Document success id : {},version:  {}", indexResponse.getId(), indexResponse.getVersion());
                return true;
            }
        } catch (Exception e) {
            log.error("insert document fail : {}", e);
        }
        return false;
    }

    /**
     * 添加/更新文档
     *
     * @param indexName
     * @param t
     * @return
     */
    protected boolean insertDocumentAutoId(String indexName, T t) {

        try {
            IndexRequest indexRequest = new IndexRequest(indexName)
                    .source(JSONObject.toJSONString(t));
            IndexResponse indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
            if (indexResponse.getResult().name().equalsIgnoreCase("created")) {
                log.info("create Document success id : {},version:  {}", indexResponse.getId(), indexResponse.getVersion());
                return true;
            }
            if (indexResponse.getResult().name().equalsIgnoreCase("updated")) {
                log.info("updated Document success id : {},version:  {}", indexResponse.getId(), indexResponse.getVersion());
                return true;
            }
        } catch (Exception e) {
            log.error("insert document fail : {}", e);
        }
        return false;
    }

    /**
     * 根据Id查找文档
     *
     * @param indexName
     * @param documentId
     * @param includes   @see org.elasticsearch.common.Strings
     * @param excludes   @see org.elasticsearch.common.Strings
     * @param clazz      映射字节码
     * @return
     */
    protected T getDocumentById(String indexName, String documentId, String[] includes, String[] excludes, Class<T> clazz) {

        try {
            GetRequest getRequest = new GetRequest(indexName, documentId);
            FetchSourceContext fetchSourceContext =
                    new FetchSourceContext(true, includes, excludes);
            getRequest.fetchSourceContext(fetchSourceContext);
            GetResponse getResponse = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
            if (getResponse.isExists()) {
                log.info("getDocumentById success id : {},version:  {}", getResponse.getId(), getResponse.getVersion());
                return JSONObject.parseObject(getResponse.getSourceAsString(), clazz);
            }
        } catch (Exception e) {
            log.error("getDocumentById fail : {}", e);
        }
        return null;
    }

    /**
     * 判断文档是否存在
     *
     * @param indexName
     * @param documentId
     * @return
     */
    protected boolean existsDocument(String indexName, String documentId) {
        try {
            GetRequest getRequest = new GetRequest(
                    indexName, documentId);
            getRequest.fetchSourceContext(new FetchSourceContext(false));
            getRequest.storedFields("_none_");
            boolean exists = restHighLevelClient.exists(getRequest, RequestOptions.DEFAULT);
            if (exists) {
                log.error("index[{}] document[{}] exists", indexName, documentId);
                return true;
            }
            log.error("index[{}] document[{}] not exists", indexName, documentId);
        } catch (Exception e) {
            log.error("existsDocument fail : {}", e);
        }
        return false;
    }

    /**
     * 修改文档
     *
     * @param indexName
     * @param documentId
     * @return
     */
    protected boolean updateDocument(String indexName, String documentId, Object... objects) {
        try {
            UpdateRequest request = new UpdateRequest(indexName, documentId).doc(objects);
            UpdateResponse updateResponse = restHighLevelClient.update(request, RequestOptions.DEFAULT);
            if (updateResponse.getResult() == DocWriteResponse.Result.CREATED) {
                log.info("create document index:{},id:{},version:{}", updateResponse.getIndex(), updateResponse.getId(), updateResponse.getVersion());
                return true;
            }
            if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                log.info("update document index:{},id:{},version:{}", updateResponse.getIndex(), updateResponse.getId(), updateResponse.getVersion());
                return true;
            }
            if (updateResponse.getResult() == DocWriteResponse.Result.DELETED) {
                log.info("delete document index:{},id:{},version:{}", updateResponse.getIndex(), updateResponse.getId(), updateResponse.getVersion());
                return true;
            }
            if (updateResponse.getResult() == DocWriteResponse.Result.NOOP) {
                log.info("noop document index:{},id:{},version:{}", updateResponse.getIndex(), updateResponse.getId(), updateResponse.getVersion());
            }
        } catch (Exception e) {
            log.error("updateDocument fail : {}", e);
        }
        return false;
    }

    /**
     * 删除文档
     *
     * @param indexName
     * @param documentId
     * @throws IOException
     */
    protected boolean deleteDocument(String indexName, String documentId) {
        try {
            DeleteRequest request = new DeleteRequest(indexName, documentId);
            DeleteResponse deleteResponse = restHighLevelClient.delete(request, RequestOptions.DEFAULT);
            if (deleteResponse.getResult() == DocWriteResponse.Result.DELETED) {
                log.info("delete document success index:{},id:{},version:{}", deleteResponse.getIndex(), deleteResponse.getId(), deleteResponse.getVersion());
                return true;
            }
        } catch (Exception e) {
            log.error("updateDocument fail : {}", e);
        }
        return false;
    }

    /**
     * 高亮分页搜索-精准匹配关键字
     * @param indexName
     * @param page
     * @param size
     * @param fieldKey 查找字段名称
     * @param fieldValue
     * @return
     */
    public JSONObject searchHighPage(String indexName,Integer page, Integer size,String fieldKey,String fieldValue) {
        try {
            SearchRequest searchRequest = new SearchRequest(indexName);
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.from(page);
            sourceBuilder.size(size);

            TermQueryBuilder termQuery = QueryBuilders.termQuery(fieldKey, fieldValue);
            sourceBuilder.query(termQuery);
            sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
            HighlightBuilder highlightBuilder = new HighlightBuilder();
            highlightBuilder.field(new HighlightBuilder.Field(fieldKey));
            highlightBuilder.requireFieldMatch(false);
            highlightBuilder.preTags("<span style='color:red'>");
            highlightBuilder.postTags("</span>");
            sourceBuilder.highlighter(highlightBuilder);
            searchRequest.source(sourceBuilder);
            SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

            CountRequest countRequest = new CountRequest(indexName);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(termQuery);
            countRequest.source(searchSourceBuilder);
            long totalCount = restHighLevelClient.count(countRequest, RequestOptions.DEFAULT).getCount();
            log.info("totalCount : {}", totalCount);

            List<Map<String,Object>> list = new ArrayList<>();
            for (SearchHit hit : response.getHits().getHits()) {
                Map<String, HighlightField> highlightFields = hit.getHighlightFields();
                HighlightField title = highlightFields.get(fieldKey);
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                if (title != null) {
                    Text[] fragments = title.fragments();
                    String n_title = "";
                    for (Text text : fragments) {
                        n_title += text;
                    }
                    sourceAsMap.put("title", n_title);
                }
                list.add(sourceAsMap);
            }
            JSONObject queryData = new JSONObject();
            queryData.put("totalCount",totalCount);
            queryData.put("dataList",list);
            return  queryData;
        } catch (Exception e) {
            log.error("searchHighPage fail : {}", e);
        }
        return null;
    }

    /**
     * 分页搜索-精准匹配关键字
     * @param indexName
     * @param page
     * @param size
     * @param fieldKey
     * @param fieldValue
     * @param clazz 返回对象
     * @return
     */
    public JSONObject searchPage(String indexName,Integer page, Integer size,String fieldKey,String fieldValue,Class<T> clazz) {

        try {
            SearchRequest searchRequest = new SearchRequest(indexName);
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.from(page);
            sourceBuilder.size(size);

            TermQueryBuilder termQuery = QueryBuilders.termQuery(fieldKey, fieldValue);
            sourceBuilder.query(termQuery);
            sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
            searchRequest.source(sourceBuilder);
            SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

            CountRequest countRequest = new CountRequest(indexName);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(termQuery);
            countRequest.source(searchSourceBuilder);
            long totalCount = restHighLevelClient.count(countRequest, RequestOptions.DEFAULT).getCount();
            log.info("totalCount : {}", totalCount);

            ArrayList<T> list = new ArrayList<>();
            for (SearchHit hit : response.getHits().getHits()) {
                list.add(JSONObject.parseObject(hit.getSourceAsString(), clazz));
            }
            JSONObject queryData = new JSONObject();
            queryData.put("totalCount",totalCount);
            queryData.put("dataList",list);
            return  queryData;
        } catch (Exception e) {
            log.error("searchPage fail : {}", e);
        }
        return null;
    }
}
