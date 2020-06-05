package org.apache.flink.table.runtime

import org.apache.flink.table.types.RowDataType
import org.apache.flink.table.utils.JsonUtils
import org.apache.flink.types.Row

class DynamicCRowWrappingCollector extends CRowWrappingCollector {

  var outRow: Row = new Row(3)

  def setRowType(rowType: RowDataType): Unit = {
    outRow.setField(0, rowType)
  }

  def collect(schema: String): Unit = {
    outRow.setField(1, true)
    outRow.setField(2, schema)
    outCRow.row = outRow
    out.collect(outCRow)
  }

  override def collect(record: Row): Unit = {
    val data = JsonUtils.toJson(record)
    outRow.setField(1, outCRow.change)
    outRow.setField(2, data)
    outCRow.row = outRow
    out.collect(outCRow)
  }

  override def close(): Unit = {
    out.close()
  }

}
