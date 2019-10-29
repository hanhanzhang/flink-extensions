package com.sdu.flink.utils;

import com.google.gson.Gson;
import java.lang.reflect.Type;

public class JsonUtils {

  private static final Gson GSON = new Gson();


  public static <T> T fromJson(String json, Class<T> clazz) {
    return GSON.fromJson(json, clazz);
  }

  public static <T> T fromJson(String json, Type type) {
    return GSON.fromJson(json, type);
  }

  public static String toJson(Object object) {
    return GSON.toJson(object);
  }



}
