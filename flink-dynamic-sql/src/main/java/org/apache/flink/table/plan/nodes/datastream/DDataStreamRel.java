package org.apache.flink.table.plan.nodes.datastream;

import java.util.Map;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.planner.DStreamPlanner;
import org.apache.flink.table.plan.nodes.FlinkRelNode;
import org.apache.flink.types.DStreamRecord;

public interface DDataStreamRel extends FlinkRelNode {

  DataStream<DStreamRecord> translateToSqlElement(DStreamPlanner tableEnv, StreamQueryConfig queryConfig);

  Map<String, String> getStreamNodeSchema();

}
