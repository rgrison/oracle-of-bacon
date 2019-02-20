package com.serli.oracle.of.bacon.loader.elasticsearch;

import com.serli.oracle.of.bacon.repository.ElasticSearchRepository;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

public class CompletionLoader {
    private static AtomicInteger count = new AtomicInteger(0);
    private static int successCount = 0;
    private static int BULK_SIZE = 100000;
    private static RestHighLevelClient client = null;
    private static BulkRequest request = null;

    private static void doRequest() {
        try {
            BulkResponse bulkResponse = client.bulk(request);
            successCount += request.numberOfActions();
            System.out.println(successCount + " actors inserted");
            if(bulkResponse.hasFailures()) {
                System.out.println("Something went wrong");
            }
        } catch (Exception e) {
            System.out.println("Everything went wrong");
        } finally {
            request = new BulkRequest();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
    	if (args.length != 1) {
    		System.err.println("Expecting 1 arguments, actual : " + args.length);
    		System.err.println("Usage : completion-loader <actors file path>");
    		System.exit(-1);
    	}
    	
        client = ElasticSearchRepository.createClient();
        request = new BulkRequest();

        String inputFilePath = args[0];
        try (BufferedReader bufferedReader = Files.newBufferedReader(Paths.get(inputFilePath))) {
            bufferedReader
                    .lines()
                    .forEach(line -> {
                        if(count.get() == 0) {
                            count.getAndIncrement();
                        } 
                        else {
                        	// pour la suggestion on indexe le nom et un champ name_suggest qui est un tableau
                        	// contenant le nom de l'acteur brut et son nom dans l'autre sens
                        	String[] input = line.split(",");
                        	String suggestionArray = "[";
                        	suggestionArray += "\"" + line.replace("\"", "") + "\"";
                        	
                        	if (input.length == 2) {
                        		suggestionArray += ", \"" + input[1].replace("\"", "") + ", " + input[0].replace("\"", "") + "\"";
                        	}
                        	
                        	suggestionArray += "]";
                        	
                            String jsonString = "{ \"name_suggest\": {\"input\": " + suggestionArray + " }, \"name\": \""+ line.replace("\"", "") + "\"}";
                            request.add(
                                new IndexRequest("actors")
                                    .id(Integer.toString(count.getAndIncrement()))
                                    .type("_doc")
                                    .source(jsonString, XContentType.JSON)
                            );
                            if(count.get() % BULK_SIZE == 0) {
                                doRequest();
                            }
                        }
                    });
        }

        doRequest();
        
        System.out.println("Inserted total of " + successCount + " actors");

        client.close();
    }
}
