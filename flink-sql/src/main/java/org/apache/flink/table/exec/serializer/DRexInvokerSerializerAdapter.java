package org.apache.flink.table.exec.serializer;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.sdu.flink.utils.JsonSerializerAdapter;
import java.lang.reflect.Type;
import org.apache.flink.table.exec.DRexInvoker;

public class DRexInvokerSerializerAdapter implements JsonSerializerAdapter<DRexInvoker> {

  private static final String TYPE = "class";
  private static final String DATA = "data";

  @Override
  public JsonElement serialize(DRexInvoker invoker, Type typeOfSrc, JsonSerializationContext context) {
    final JsonObject jo = new JsonObject();
    jo.addProperty(TYPE, invoker.getClass().getCanonicalName());
    jo.add(DATA, context.serialize(invoker));
    return jo;
  }

  @Override
  public DRexInvoker deserialize(JsonElement element, Type typeOfT, JsonDeserializationContext context)
      throws JsonParseException {
    final JsonObject jo = (JsonObject) element;
    final String className = jo.get(TYPE).getAsString();
    final JsonElement data = jo.get(DATA);
    return deserialize(className, data, context);
  }

  private DRexInvoker deserialize(String className, JsonElement data, JsonDeserializationContext context)
      throws JsonParseException {
    try {
      return context.deserialize(data, Class.forName(className));
    } catch (Exception e) {
      throw new JsonParseException("deserialize failure", e);
    }
  }

  @Override
  public Class<DRexInvoker> serializerAdapterType() {
    return DRexInvoker.class;
  }
}
