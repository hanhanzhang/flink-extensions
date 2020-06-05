package org.apache.flink.table.plan.nodes.datastream

import org.apache.flink.table.types.schema.SqlSchema

trait DynamicStreamRel {

  def createSqlSchema: SqlSchema

}
