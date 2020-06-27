package org.apache.flink.table.runtime

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.table.util.JsonUtils
import org.apache.flink.types.Row

class DynamicSqlSinkMapFunction extends RichMapFunction[CRow, CRow] {

  @transient var outCRow: CRow = _


  override def open(parameters: Configuration): Unit = {
    outCRow = new CRow(null, true)
  }

  override def map(in: CRow): CRow = {
    outCRow.change = in.change
    outCRow.row = JsonUtils.fromJson(in.row.getField(1).asInstanceOf[String], classOf[Row])
    outCRow
  }

}
