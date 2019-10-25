package org.apache.flink.table.codegen;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.types.DRecordTuple;

public interface DExpressionInvoker<T> {

  T invoke(DRecordTuple recordTuple) throws DExpressionInvokeException;

  SqlTypeName getResultType();

}
