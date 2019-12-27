package com.sdu.flink.runtime.network;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.util.AbstractID;

public class NetworkConstants {

  // 最大并发度
  public static int TASK_MAX_PARALLELISM = 128;

  public static int UPSTREAM_TM_BIND_PORT = 6715;
  public static int DOWNSTREAM_TM_BIND_PORT = 6716;

  private static final ExecutionAttemptID UPSTREAM_TASK_ID = new ExecutionAttemptID(31, 32);

  public static final IntermediateDataSetID UPSTREAM_IDS_ID = new IntermediateDataSetID(new AbstractID(100, 101));

  private static final IntermediateResultPartitionID UPSTREAM_IRP_ID = new IntermediateResultPartitionID(201, 202);
  public static final ResultPartitionID UPSTREAM_RP_ID = new ResultPartitionID(UPSTREAM_IRP_ID, UPSTREAM_TASK_ID);

  public static final String UPSTREAM_TASK_NAME = "Task(1/1)";
  public static final String DOWNSTREAM_TASK_NAME = "Task(1/1)";

  private NetworkConstants() {

  }



}
