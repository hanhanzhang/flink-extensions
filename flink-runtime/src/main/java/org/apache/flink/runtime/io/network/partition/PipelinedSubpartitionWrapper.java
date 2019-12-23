package org.apache.flink.runtime.io.network.partition;

public class PipelinedSubpartitionWrapper extends PipelinedSubpartition {

  public PipelinedSubpartitionWrapper(int index, ResultPartition parent) {
    super(index, parent);
  }

}
