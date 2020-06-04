package org.apache.flink.table.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.lang.reflect.Type;

public class JsonUtils {

  private static final Gson GSON = new GsonBuilder().create();

  private JsonUtils() {

  }

  public static <T> T fromJson(String json, Class<T> clazz) {
    return GSON.fromJson(json, clazz);
  }

  public static <T> T fromJson(String json, Type type) {
    return GSON.fromJson(json, type);
  }

  public static <T> String toJson(T object) {
    return GSON.toJson(object);
  }
}
