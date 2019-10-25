package com.sdu.flink.utils;

import com.google.gson.Gson;

public class JsonUtils {

  private static final Gson GSON = new Gson();


  public static <T> T fromJson(String json, Class<T> clazz) {
    return GSON.fromJson(json, clazz);
  }

  public static String toJson(Object object) {
    return GSON.toJson(object);
  }
}
