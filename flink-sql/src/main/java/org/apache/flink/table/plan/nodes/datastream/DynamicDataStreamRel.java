package org.apache.flink.table.plan.nodes.datastream;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.delegation.DynamicStreamPlanner;
import org.apache.flink.table.plan.nodes.FlinkRelNode;
import org.apache.flink.types.SimpleSqlElement;

public interface DynamicDataStreamRel extends FlinkRelNode {

  DataStream<SimpleSqlElement> translateToSqlElement(DynamicStreamPlanner tableEnv, StreamQueryConfig queryConfig);

}
