package org.apache.flink.table.codegen;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.annotation.Internal;

@Internal
public interface DRexFilterInvoker extends DRexInvoker<Boolean> {

  @Override
  default SqlTypeName getResultType() {
    return SqlTypeName.BOOLEAN;
  }

}
