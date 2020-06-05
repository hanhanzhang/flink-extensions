package org.apache.flink.table.runtime

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.table.types.RowDataType

class DynamicSqlSinkFilterFunction extends FilterFunction[CRow] {

  override def filter(value: CRow): Boolean = {
    value.row.getField(0).equals(RowDataType.DATA.name())
  }

}
