package org.apache.flink.table.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.internal.bind.ObjectTypeAdapter;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonUtils {

  private static final Logger LOG = LoggerFactory.getLogger(JsonUtils.class);

  private static final Gson GSON;

  static {
    GsonBuilder gb = new GsonBuilder();

    ServiceLoader<JsonSerializerAdapter> serviceLoader = ServiceLoader.load(JsonSerializerAdapter.class);
    for (JsonSerializerAdapter adapter : serviceLoader) {
      gb.registerTypeAdapter(adapter.serializerAdapterType(), adapter);
    }

    GSON = gb.create();

    // 解决反序列化: int --> double 问题
    try {
      Field factories = Gson.class.getDeclaredField("factories");
      factories.setAccessible(true);
      Object o = factories.get(GSON);
      Class<?>[] declaredClasses = Collections.class.getDeclaredClasses();
      for (Class c : declaredClasses) {
        if ("java.util.Collections$UnmodifiableList".equals(c.getName())) {
          Field listField = c.getDeclaredField("list");
          listField.setAccessible(true);
          @SuppressWarnings("unchecked")
          List<TypeAdapterFactory> list = (List<TypeAdapterFactory>) listField.get(o);
          int i = list.indexOf(ObjectTypeAdapter.FACTORY);
          list.set(i, FlinkObjectTypeAdapter.FACTORY);
          break;
        }
      }
    } catch (Exception e) {
      // ignore
      LOG.error("change object serializer / deserializer failure", e);
    }
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
