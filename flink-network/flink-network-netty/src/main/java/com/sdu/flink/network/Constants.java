package com.sdu.flink.network;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.util.AbstractID;

public class Constants {

  static int TASK_MANAGER_PORT_1 = 8912;
  static int TASK_MANAGER_PORT_2 = 8913;

  static final AbstractID TASK_MANAGER_ID_1 = new AbstractID(11, 12);
  static final AbstractID TASK_MANAGER_ID_2 = new AbstractID(21, 22);

  static final ExecutionAttemptID TASK_EXECUTION_ATTEMPT_1 = new ExecutionAttemptID(31, 32);
  static final ExecutionAttemptID TASK_EXECUTION_ATTEMPT_2 = new ExecutionAttemptID(51, 52);

  /*
   *
   * **/
  static IntermediateDataSetID IDS = new IntermediateDataSetID(new AbstractID(100, 101));
  static IntermediateResultPartitionID IRP = new IntermediateResultPartitionID(201, 202);
  public static final ResultPartitionID RP = new ResultPartitionID(IRP, TASK_EXECUTION_ATTEMPT_1);

}
