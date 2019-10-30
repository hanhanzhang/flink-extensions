package com.sdu.flink.utils;

import com.google.gson.JsonDeserializer;
import com.google.gson.JsonSerializer;

public interface JsonSerializerAdapter<T> extends JsonSerializer<T>, JsonDeserializer<T> {

  Class<T> serializerAdapterType();

}
