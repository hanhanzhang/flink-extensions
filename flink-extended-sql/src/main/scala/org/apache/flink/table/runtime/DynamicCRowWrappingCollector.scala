package org.apache.flink.table.runtime

import org.apache.flink.table.util.JsonUtils
import org.apache.flink.types.Row

class DynamicCRowWrappingCollector extends CRowWrappingCollector {

  var outRow: Row = new Row(2)

  def setRowType(rowType: String): Unit = {
    outRow.setField(0, rowType)
  }

  def collect(schema: String): Unit = {
    outRow.setField(1, schema)
    outCRow.row = outRow
    out.collect(outCRow)
  }

  override def collect(record: Row): Unit = {
    val data = JsonUtils.toJson(record)
    outRow.setField(1, data)
    outCRow.row = outRow
    out.collect(outCRow)
  }

  override def close(): Unit = {
    out.close()
  }

}
