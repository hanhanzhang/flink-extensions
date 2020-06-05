package org.apache.flink.table.runtime

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.table.utils.JsonUtils
import org.apache.flink.types.Row

class DynamicSqlSinkMapFunction extends MapFunction[CRow, CRow]{

  private val outCRow = new CRow(null, true)

  override def map(in: CRow): CRow = {
    val change = in.row.getField(1).asInstanceOf
    val rowData = in.row.getField(2).asInstanceOf
    outCRow.change = change
    outCRow.row = JsonUtils.fromJson(rowData, classOf[Row])
    outCRow
  }

}
