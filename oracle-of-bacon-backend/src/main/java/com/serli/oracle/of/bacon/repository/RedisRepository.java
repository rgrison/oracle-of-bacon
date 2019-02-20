package com.serli.oracle.of.bacon.repository;

import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.List;

public class RedisRepository {
    private static final int HISTORY_MAX_LENGTH = 9;
    
	private final Jedis jedis;
    private final static String KEY = "SearchHistory";

    public RedisRepository() {
        this.jedis = new Jedis("localhost");
    }

    public void saveSearch(String query) {
        jedis.lpush(KEY, query);
        jedis.ltrim(KEY, 0, HISTORY_MAX_LENGTH);
    }

    public List<String> getLastTenSearches() {
        String[] last10 = jedis.lrange(KEY, 0, -1).toArray(new String[0]);
        return Arrays.asList(last10);
    }
}
