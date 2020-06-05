package org.apache.flink.table.api;

import org.apache.calcite.rel.RelNode;

public class DynamicStreamNameUtils {

  private DynamicStreamNameUtils() {

  }

  public static String getStreamNodeUniqueName(RelNode relNode) {
    throw new RuntimeException();
  }

}
