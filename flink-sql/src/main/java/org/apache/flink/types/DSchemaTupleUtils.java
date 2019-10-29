package org.apache.flink.types;

import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.plan.nodes.datastream.DDataStreamRel;

/**
 * @author hanhan.zhang
 * */
public class DSchemaTupleUtils {

  private DSchemaTupleUtils() {

  }

  public static String getStreamNodePath(RelNode relNode) {
    if (relNode.getInputs() == null || relNode.getInputs().isEmpty()) {
      return relNode.getClass().getSimpleName();
    }

    StringBuilder sb = new StringBuilder();
    sb.append(relNode.getClass().getSimpleName());
    for (RelNode node : relNode.getInputs()) {
      sb.append("#");
      sb.append(getStreamNodePath(node));
    }
    return sb.toString();
  }

  public static void toSchemaTuple(DDataStreamRel streamRel, DSchemaTuple schemaTuple) {
    if (streamRel.getInputs() == null || streamRel.getInputs().isEmpty()) {
      schemaTuple.addStreamNodeSchema(getStreamNodePath(streamRel), streamRel.getStreamNodeSchema());
      return;
    }
    schemaTuple.addStreamNodeSchema(getStreamNodePath(streamRel), streamRel.getStreamNodeSchema());
    for (RelNode relNode : streamRel.getInputs()) {
      if (relNode instanceof DDataStreamRel) {
        toSchemaTuple((DDataStreamRel) relNode, schemaTuple);
        continue;
      }
      throw new UnsupportedRelNodeException("Unsupported RelNode: " + relNode.getClass().getSimpleName());
    }
  }
}
