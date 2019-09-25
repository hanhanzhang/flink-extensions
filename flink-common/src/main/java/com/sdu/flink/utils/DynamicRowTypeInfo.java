package com.sdu.flink.utils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.RowSerializer;
import org.apache.flink.types.Row;

public class DynamicRowTypeInfo extends RowTypeInfo {

  public DynamicRowTypeInfo(TypeInformation<?>[] types, String[] fieldNames) {
    super(types, fieldNames);
  }

  @Override
  public TypeSerializer<Row> createSerializer(ExecutionConfig config) {
    int len = getArity() + 1;
    TypeSerializer<?>[] fieldSerializers = new TypeSerializer[len];
    for (int i = 0; i < len; i++) {
      if (i == 0) {
        fieldSerializers[i] = Types.BOOLEAN.createSerializer(config);
      } else {
        fieldSerializers[i] = types[i - 1].createSerializer(config);
      }

    }
    return new RowSerializer(fieldSerializers);
  }
}
