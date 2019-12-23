package com.sdu.flink.runtime.network.partition;

import static com.sdu.flink.runtime.network.NetworkConstants.RP;
import static com.sdu.flink.runtime.network.NetworkConstants.TASK_MAX_PARALLELISM;
import static com.sdu.flink.runtime.network.NetworkConstants.UPSTREAM_TASK_NAME;
import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.PIPELINED_BOUNDED;

import com.sdu.flink.entry.UserActionEntry;
import com.sdu.flink.runtime.Bootstrap;
import com.sdu.flink.runtime.network.SendEndpointBufferPoolFactory;
import com.sdu.flink.runtime.taskexecutor.TaskManagerBootstrap;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterBuilder;
import org.apache.flink.runtime.io.network.partition.PipelinedSubpartitionWrapper;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;

public class UpstreamTMTaskBootstrap implements Bootstrap {

  private UpstreamStreamRecordEmitter emitter;

  @SuppressWarnings("unchecked")
  @Override
  public void setup(Configuration configuration) throws Exception {
    configuration.setInteger(NettyShuffleEnvironmentOptions.DATA_PORT, 6715);
    configuration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 1);

    // 初始化TaskManager进程组件
    TaskManagerBootstrap taskManagerBootstrap = new TaskManagerBootstrap();
    taskManagerBootstrap.setup(configuration);

    // 初始化Task线程组件
    /*
     * Task组件
     *
     * 1 RecordWriter
     *
     *   RecordWriter负责向下游Task写数据, 其数据存储在ResultPartition
     *
     * 2 ResultPartition
     *
     *   ResultPartitionFactory负责初始化ResultPartition
     * */
    int numberOfSubpartitions = 2;
    int networkBuffersPerChannel = configuration.getInteger(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_PER_CHANNEL);
    int floatingNetworkBuffersPerGate = configuration.getInteger(NettyShuffleEnvironmentOptions.NETWORK_EXTRA_BUFFERS_PER_GATE);
    ResultPartitionType type = PIPELINED_BOUNDED;
    SendEndpointBufferPoolFactory bufferPoolFactory = new SendEndpointBufferPoolFactory(numberOfSubpartitions,
        networkBuffersPerChannel, floatingNetworkBuffersPerGate, type, taskManagerBootstrap.getNetworkBufferPool());
    ResultSubpartition[] subpartitions = new ResultSubpartition[2];
    ResultPartition resultPartition = new ResultPartition(UPSTREAM_TASK_NAME, RP,
        type, subpartitions, TASK_MAX_PARALLELISM, taskManagerBootstrap.getResultPartitionManager(),
        bufferPoolFactory);
    // 构建下游每个Task的中间结果数据集
    for (int i = 0; i < subpartitions.length; ++i) {
      subpartitions[i] = new PipelinedSubpartitionWrapper(i, resultPartition);
    }
    resultPartition.setup();

    KeySelector<UserActionEntry, Integer> selector = UserActionEntry::getUid;
    RecordWriter<SerializationDelegate<UserActionEntry>> writer = new RecordWriterBuilder()
        .setTaskName(UPSTREAM_TASK_NAME)
        .setChannelSelector(new KeyGroupStreamPartitioner(selector, TASK_MAX_PARALLELISM))
        .build(resultPartition);

    emitter = new UpstreamStreamRecordEmitter(writer);
  }

  @Override
  public void start() throws Exception {
    this.emitter.start();
  }

  @Override
  public void stop() throws Exception {
    this.emitter.close();
  }

  public static void main(String[] args) {

  }

}
