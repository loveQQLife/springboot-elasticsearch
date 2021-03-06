package com.kraken;

import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.search.TotalHits;
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
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = SpringbootEsApplication.class)
@Slf4j
public class SpringbootEsApplicationTests {

	@Autowired
	private RestHighLevelClient client;

	@Test
	public void testCreateIndex() throws IOException {
		CreateIndexRequest request = new CreateIndexRequest("test1");
		request.settings(Settings.builder()
				.put("index.number_of_shards", 5)
				.put("index.number_of_replicas", 1)
		);

		Map<String, Object> properties = new HashMap<>();
		Map<String, Object> mapping = new HashMap<>();
		mapping.put("properties", properties);
		request.mapping(mapping);

		CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
		if (createIndexResponse.isAcknowledged() && createIndexResponse.isShardsAcknowledged()){
			System.out.println(true);
		} else {
			System.out.println(false);
		}
		System.out.println(createIndexResponse.toString());
	}

	/**
	 * ????????????2
	 */
	@Test
	public void testCreateIndex2() throws IOException {
		CreateIndexRequest request = new CreateIndexRequest("test");
		request.settings(Settings.builder()
				.put("index.number_of_shards", 5)
				.put("index.number_of_replicas", 1)
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

		CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);

		System.out.println(createIndexResponse.index());
	}

	/**
	 * ????????????
	 */
	@Test
	public void testDeleteIndex() throws IOException {
		DeleteIndexRequest request = new DeleteIndexRequest("test1");
		AcknowledgedResponse deleteIndexResponse = client.indices().delete(request, RequestOptions.DEFAULT);
		System.out.println(deleteIndexResponse.isAcknowledged());
	}

	/**
	 * ????????????????????????
	 */
	@Test
	public void testExistsIndex() throws IOException {
		GetIndexRequest request = new GetIndexRequest("book");
		boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
		System.out.println(exists);
	}

	/**
	 * ???????????????mapping
	 * throws IOException
	 */
	@Test
	public void testGetMapping() throws IOException {
		GetMappingsRequest request = new GetMappingsRequest();
		request.indices("test");
		GetMappingsResponse getMappingResponse = client.indices().getMapping(request, RequestOptions.DEFAULT);

		Map<String, MappingMetaData> allMappings = getMappingResponse.mappings();
		MappingMetaData indexMapping = allMappings.get("test");
		Map<String, Object> mapping = indexMapping.sourceAsMap();

		for(String s:mapping.keySet()){
			System.out.println("key : "+s+" value : "+mapping.get(s));
		}

	}

	/**
	 * ???????????????Settings??????
	 */
	@Test
	public void testGetSettings() throws IOException {

		GetSettingsRequest request = new GetSettingsRequest().indices("book");

		GetSettingsResponse getSettingsResponse = client.indices().getSettings(request, RequestOptions.DEFAULT);

		String numberOfShardsString = getSettingsResponse.getSetting("book", "index.number_of_shards");

		String numberOfReplicasString = getSettingsResponse.getSetting("book", "index.number_of_replicas");

		System.out.println(numberOfShardsString);
		System.out.println(numberOfReplicasString);

	}

	/**
	 * ???????????? Document
	 */
	@Test
	public void testInsertDocument() throws IOException {

			Map<String, Object> jsonMap = new HashMap<>();
			jsonMap.put("number", 15L);
			jsonMap.put("create_time", new Date());
			jsonMap.put("price", 356.5d);
			jsonMap.put("name", "zhang");
			jsonMap.put("title", "????????????00");
			IndexRequest indexRequest = new IndexRequest("test").id("gZpUC3oB_7z_Coizt5ni")
					.source(jsonMap);
			IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
			log.info("indexResponse:{}",indexResponse.toString());

	}

	@Test
	public void testGetDocumentById() throws IOException {
		GetRequest getRequest = new GetRequest("book", "1111");
        /*
         * ??????????????????
         */
        String[] includes = new String[]{"name","title"};
        String[] excludes = Strings.EMPTY_ARRAY;
        FetchSourceContext fetchSourceContext =
                new FetchSourceContext(true, includes, excludes);
        getRequest.fetchSourceContext(fetchSourceContext);

        /*
          ??????????????????
         */
//		String[] includes = Strings.EMPTY_ARRAY;
//		String[] excludes = new String[]{"number"};
//		FetchSourceContext fetchSourceContext =
//				new FetchSourceContext(true, includes, excludes);
//		getRequest.fetchSourceContext(fetchSourceContext);

		GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);

		String index = getResponse.getIndex();
		String id = getResponse.getId();
		if (getResponse.isExists()) {
			long version = getResponse.getVersion();
			log.info("??????????????????version:{}",version);
			String sourceAsString = getResponse.getSourceAsString();
			log.info("????????????string?????????sourceAsString:{}",sourceAsString);
			Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
			log.info("????????????map?????????sourceAsMap:{}",sourceAsMap);
			byte[] sourceAsBytes = getResponse.getSourceAsBytes();
			log.info("???????????????sourceAsBytes:{}",sourceAsBytes);
		} else {
			log.info("????????????");
		}
		log.info("index:{}",index);
		log.info("id:{}",id);
	}

	/**
	 * ????????????????????????
	 */
	@Test
	public void testExistsDocument() throws IOException {
		GetRequest getRequest = new GetRequest(
				"yuan",
				"10");
		//????????????_source
		getRequest.fetchSourceContext(new FetchSourceContext(false));
		//??????????????????????????????
		getRequest.storedFields("_none_");

		boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);

		System.out.println(exists);
	}


	/**
	 * ????????????
	 */
	@Test
	public void testUpdateDocument() throws IOException {
		UpdateRequest request = new UpdateRequest("test", "gZpUC3oB_7z_Coizt5ni")
				.doc("name","????????????(update)","title","???(update)?????????????????????3");
		UpdateResponse updateResponse = client.update(
				request, RequestOptions.DEFAULT);
		String index = updateResponse.getIndex();
		log.info("index:{}",index);
		String id = updateResponse.getId();
		log.info("id:{}",id);
		long version = updateResponse.getVersion();
		log.info("version:{}",version);

		if (updateResponse.getResult() == DocWriteResponse.Result.CREATED) {
			log.info("?????????????????????????????????");
		} else if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
			log.info("???????????????????????????");
		} else if (updateResponse.getResult() == DocWriteResponse.Result.DELETED) {
			log.info("??????????????????????????????");
		} else if (updateResponse.getResult() == DocWriteResponse.Result.NOOP) {
			log.info("???????????????????????????????????????????????????????????????????????????????????????");
		}
	}

	/**
	 * ????????????
	 */
	@Test
	public void testDeleteDocument() throws IOException {
		DeleteRequest request = new DeleteRequest("test", "gZpUC3oB_7z_Coizt5ni");
		DeleteResponse deleteResponse = client.delete(request, RequestOptions.DEFAULT);
		log.info("deleteResponse:{}",deleteResponse);
	}

	/**
	 * ????????????
	 */
	@Test
	public void testSearch() throws IOException {
		SearchRequest searchRequest = new SearchRequest("book");
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(QueryBuilders.matchQuery("name","hua"));
		searchRequest.source(searchSourceBuilder);
		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
		float maxScore = searchResponse.getHits().getMaxScore();
		TotalHits totalHits = searchResponse.getHits().getTotalHits();
		SearchHit[] hits = searchResponse.getHits().getHits();

		CountRequest countRequest = new CountRequest("book");
		SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
		sourceBuilder.query(QueryBuilders.matchQuery("name","hua"));
		countRequest.source(sourceBuilder);
		long totalCount = client.count(countRequest, RequestOptions.DEFAULT).getCount();
		log.info("totalCount : {}",totalCount );


		for (SearchHit hit : hits) {
			log.info("id:{}", hit.getId());
			log.info("?????????:{}", hit.getIndex());
			log.info("??????:{}", hit.getScore());
			log.info("string:{}", hit.getSourceAsString());
			log.info("map:{}", hit.getSourceAsMap());
		}

		log.info("totalHits value:{}",totalHits.value);
		log.info("totalHits relation:{}",totalHits.relation);
		log.info("maxScore:{}",maxScore);
		log.info("searchResponse:{}",searchResponse);
	}


	@Test
	public void testSearch2() throws IOException {
		SearchRequest searchRequest = new SearchRequest("book");
		SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
		sourceBuilder.query(QueryBuilders.matchQuery("title","kraken"));
		sourceBuilder.from(0);
		sourceBuilder.size(1);
		sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
		searchRequest.source(sourceBuilder);

		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
		SearchHit[] hits;
		hits = searchResponse.getHits().getHits();

		for (SearchHit hit : hits) {
			log.info("id:{}", hit.getId());
			log.info("?????????:{}", hit.getIndex());
			log.info("??????:{}", hit.getScore());
			log.info("string:{}", hit.getSourceAsString());
			log.info("map:{}", hit.getSourceAsMap());
		}
	}

	@Test
	public void testSearchHighPage(){
		try {
			List<Map<String,Object>> list = this.searchHighPage("",0,20);
			System.out.println(list);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/**
	 * ??????????????????
	 * @param keyWord
	 * @param page
	 * @param size
	 * @return
	 * @throws IOException
	 */
	public List<Map<String,Object>> searchHighPage(String keyWord, Integer page, Integer size) throws IOException {
		SearchRequest searchRequest = new SearchRequest("book");
		SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
		sourceBuilder.from(page);
		sourceBuilder.size(size);

		//?????????????????????
		TermQueryBuilder termQuery = QueryBuilders.termQuery("name", keyWord);
		sourceBuilder.query(termQuery);
		sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
		HighlightBuilder highlightBuilder = new HighlightBuilder();
		highlightBuilder.field(new HighlightBuilder.Field("name"));
		highlightBuilder.requireFieldMatch(false);
		highlightBuilder.preTags("<span style='color:red'>");
		highlightBuilder.postTags("</span>");
		sourceBuilder.highlighter(highlightBuilder);

		searchRequest.source(sourceBuilder);
		SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

		CountRequest countRequest = new CountRequest("book");
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(termQuery);
		countRequest.source(searchSourceBuilder);
		long totalCount = client.count(countRequest, RequestOptions.DEFAULT).getCount();
		log.info("totalCount : {}",totalCount );

		ArrayList<Map<String,Object>> list = new ArrayList<>();
		//????????????
		for (SearchHit hit : response.getHits().getHits()) {
			//??????????????????
			Map<String, HighlightField> highlightFields = hit.getHighlightFields();
			HighlightField title = highlightFields.get("title");

			Map<String, Object> sourceAsMap = hit.getSourceAsMap();

			if (title!=null){
				Text[] fragments = title.fragments();
				String n_title="";
				for (Text text : fragments) {
					n_title+=text;
				}
				sourceAsMap.put("title",n_title);
			}

			list.add(sourceAsMap);
		}
		return list;
	}


}
