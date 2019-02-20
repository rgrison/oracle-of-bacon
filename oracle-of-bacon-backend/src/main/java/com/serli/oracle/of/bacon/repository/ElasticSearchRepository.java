package com.serli.oracle.of.bacon.repository;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.search.suggest.term.TermSuggestion;

public class ElasticSearchRepository {

    private final RestHighLevelClient client;

    public ElasticSearchRepository() {
        client = createClient();

    }

    public static RestHighLevelClient createClient() {
        return new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")
                )
        );
    }

    public List<String> getActorsSuggests(String searchQuery) throws IOException {
        // request suggestion
    	SearchRequest searchRequest = new SearchRequest("actors");
    	
    	SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		SuggestionBuilder completionSuggestionBuilder =
    	    SuggestBuilders.completionSuggestion("name_suggest").text(searchQuery).size(10); 
    	SuggestBuilder suggestBuilder = new SuggestBuilder();
    	suggestBuilder.addSuggestion("actors_completion", completionSuggestionBuilder); 
    	searchSourceBuilder.suggest(suggestBuilder);
    	
    	searchRequest.source(searchSourceBuilder);
    	
    	SearchResponse searchResponse = client.search(searchRequest);
    	
    	// retrieving suggestion
    	Suggest suggest = searchResponse.getSuggest();
    	CompletionSuggestion completionSuggestion = suggest.getSuggestion("actors_completion");
    	
    	List<String> suggestions = new LinkedList<>();
    	for (CompletionSuggestion.Entry entry : completionSuggestion.getEntries()) {
    	    for (CompletionSuggestion.Entry.Option option : entry) {
    	        suggestions.add((String) option.getHit().getSourceAsMap().get("name"));
    	    }
    	}
    	
        return suggestions;
    }
}
