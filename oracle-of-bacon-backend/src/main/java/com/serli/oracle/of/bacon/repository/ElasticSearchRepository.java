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

    private static final String FIELD_NAME = "name";
	private static final String ACTORS_COMPLETION = "actors_completion";
    private static final String SUGGESTION_FIELD = "name_suggest";
    private static final String INDEX = "actors";
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
    	SearchRequest searchRequest = new SearchRequest(INDEX);
    	
    	// Construction de la recherche pour la suggestion
    	SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		SuggestionBuilder completionSuggestionBuilder =
    	    SuggestBuilders.completionSuggestion(SUGGESTION_FIELD).text(searchQuery).size(10); 
    	SuggestBuilder suggestBuilder = new SuggestBuilder();
    	suggestBuilder.addSuggestion(ACTORS_COMPLETION, completionSuggestionBuilder); 
    	searchSourceBuilder.suggest(suggestBuilder);
    	
    	searchRequest.source(searchSourceBuilder);
    	
    	// Exécution de la recherche
    	SearchResponse searchResponse = client.search(searchRequest);
    	
    	// Récupération des suggestions
    	Suggest suggest = searchResponse.getSuggest();
    	CompletionSuggestion completionSuggestion = suggest.getSuggestion(ACTORS_COMPLETION);
    	
    	List<String> suggestions = new LinkedList<>();
    	for (CompletionSuggestion.Entry entry : completionSuggestion.getEntries()) {
    	    for (CompletionSuggestion.Entry.Option option : entry) {
    	        suggestions.add((String) option.getHit().getSourceAsMap().get(FIELD_NAME));
    	    }
    	}
    	
        return suggestions;
    }
}
