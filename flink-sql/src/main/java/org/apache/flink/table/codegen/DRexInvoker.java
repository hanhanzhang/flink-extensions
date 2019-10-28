package org.apache.flink.table.codegen;

import java.io.Serializable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.types.DRecordTuple;

public interface DRexInvoker<T> extends Serializable {

  T invoke(DRecordTuple recordTuple) throws DRexInvokeException;

  SqlTypeName getResultType();

}
