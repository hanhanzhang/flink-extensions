package org.apache.flink.table.exec;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.annotation.Internal;

@Internal
public interface DRexFilterInvoker extends DRexInvoker {

  @Override
  default SqlTypeName getResultType() {
    return SqlTypeName.BOOLEAN;
  }

}
