package com.sdu.flink.runtime.network.partition;

import java.util.function.Consumer;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;

public class MockTask implements PartitionProducerStateProvider {

  @Override
  public void requestPartitionProducerState(IntermediateDataSetID intermediateDataSetId,
      ResultPartitionID resultPartitionId, Consumer<? super ResponseHandle> responseConsumer) {

  }


}
