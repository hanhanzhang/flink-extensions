package org.apache.flink.table.exec;

import java.io.Serializable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.types.DRecordTuple;
import org.apache.flink.types.DSqlTypeUtils;

public interface DRexInvoker extends Serializable {

  Object invoke(DRecordTuple recordTuple) throws DRexInvokeException;

  SqlTypeName getResultType();

  default Object getRecordValue(String recordValue) {
    if (recordValue == null) {
      return null;
    }
    return DSqlTypeUtils.stringToObject(recordValue, getResultType());
  }

}
