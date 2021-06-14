package com.kraken;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @author kraken
 * @date 2021/6/14 16:28
 */
@Configuration
public class ElasticSearchConfig {

    @Value("${elasticsearch.cluster-nodes}")
    private String clusterNodes;

    /**
     * user high level client (suggest by formal)
     * @return
     */
    @Bean
    public RestHighLevelClient restHighLevelClient() {
        String[] node_arr = clusterNodes.split(",");
        HttpHost[] hosts = new HttpHost[node_arr.length];
        Arrays.stream(node_arr).filter(str -> str.contains(":")).collect(Collectors.toList()).forEach(forEachWithIndex((node, index) -> {
            HttpHost httpHost = new HttpHost(node.substring(0, node.lastIndexOf(":")), Integer.parseInt(node.substring(node.lastIndexOf(":") + 1)));
            hosts[index] = httpHost;
        }));
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(hosts));
        return client;
    }

    /**
     * 利用BiConsumer实现foreach循环支持index
     *
     * @param biConsumer
     * @param <T>
     * @return
     */
    public static <T> Consumer<T> forEachWithIndex(BiConsumer<T, Integer> biConsumer) {
        class IncrementInt {
            int i = 0;

            public int getAndIncrement() {
                return i++;
            }
        }
        IncrementInt incrementInt = new IncrementInt();
        return t -> biConsumer.accept(t, incrementInt.getAndIncrement());
    }
}
