package com.sdu.flink.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.lang.reflect.Type;
import java.util.ServiceLoader;

public class JsonUtils {

  private static final Gson GSON;

  static {
    GsonBuilder gb = new GsonBuilder();

    ServiceLoader<JsonSerializerAdapter> serviceLoader = ServiceLoader.load(JsonSerializerAdapter.class);
    for (JsonSerializerAdapter adapter : serviceLoader) {
      gb.registerTypeAdapter(adapter.serializerAdapterType(), adapter);
    }

    GSON = gb.create();
  }

  public static <T> T fromJson(String json, Class<T> clazz) {
    return GSON.fromJson(json, clazz);
  }

  public static <T> T fromJson(String json, Type type) {
    return GSON.fromJson(json, type);
  }

  public static String toJson(Object object) {
    return GSON.toJson(object);
  }

  public static String toJson(Object object, Type type) {
    return GSON.toJson(object, type);
  }

}
