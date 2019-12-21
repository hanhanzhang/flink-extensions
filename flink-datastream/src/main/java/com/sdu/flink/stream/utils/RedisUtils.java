package com.sdu.flink.stream.utils;

import java.util.Map;
import java.util.Map.Entry;
import redis.clients.jedis.Jedis;

public class RedisUtils {

  public static void addData(Map<String, String> data, Jedis jedis) {
    for (Entry<String, String> entry : data.entrySet()) {
      jedis.set(entry.getKey(), entry.getValue());
    }
  }

}
