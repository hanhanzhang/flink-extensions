package org.apache.flink.table.plan.nodes.datastream;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.delegation.DSqlStreamPlanner;
import org.apache.flink.table.plan.nodes.FlinkRelNode;
import org.apache.flink.types.CompositeDRow;

public interface DSqlDataStreamRel extends FlinkRelNode {

  DataStream<CompositeDRow> translateToSqlElement(DSqlStreamPlanner tableEnv, StreamQueryConfig queryConfig);

}
