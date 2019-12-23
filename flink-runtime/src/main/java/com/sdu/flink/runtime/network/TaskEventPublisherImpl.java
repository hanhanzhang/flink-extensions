package com.sdu.flink.runtime.network;

import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;

public class TaskEventPublisherImpl implements TaskEventPublisher {

  @Override
  public boolean publish(ResultPartitionID partitionId, TaskEvent event) {
    return false;
  }

}
