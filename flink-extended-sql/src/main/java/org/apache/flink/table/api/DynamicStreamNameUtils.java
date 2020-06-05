package org.apache.flink.table.api;

import org.apache.calcite.rel.RelNode;

public class DynamicStreamNameUtils {

  private DynamicStreamNameUtils() {

  }

  public static String getStreamNodeUniqueName(RelNode relNode) {
    if (relNode.getInputs() == null || relNode.getInputs().isEmpty()) {
      return relNode.getClass().getSimpleName();
    }

    StringBuilder sb = new StringBuilder();
    sb.append(relNode.getClass().getSimpleName());
    for (RelNode node : relNode.getInputs()) {
      sb.append("#");
      sb.append(getStreamNodeUniqueName(node));
    }
    return sb.toString();
  }

}
