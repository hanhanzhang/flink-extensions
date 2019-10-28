package org.apache.flink.table.codegen;

import java.io.Serializable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.types.DRecordTuple;

public interface DRexInvoker extends Serializable {

  Object invoke(DRecordTuple recordTuple) throws DRexInvokeException;

  SqlTypeName getResultType();

  // TODO: 2019-10-28 DTypeConverts 类型对齐
  default Object getRecordValue(String recordValue) {
    if (recordValue == null) {
      return null;
    }
    switch (getResultType()) {
      // String
      case VARCHAR:
        return recordValue;
      case CHAR:
        char[] chars = recordValue.toCharArray();
        assert chars.length == 1;
        return chars[0];

      // Boolean
      case BOOLEAN:
        return Boolean.parseBoolean(recordValue);

      // Number
      case SMALLINT:
        return Byte.parseByte(recordValue);
      case INTEGER:
        return Integer.parseInt(recordValue);
      case BIGINT:
        return Long.parseLong(recordValue);
      case FLOAT:
        return Float.parseFloat(recordValue);
      case DOUBLE:
        return Double.parseDouble(recordValue);

      default:
        throw new RuntimeException("Unsupported SqlType: " + getResultType());
    }
  }

}
