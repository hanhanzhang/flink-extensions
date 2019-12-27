package com.sdu.flink.runtime.network.partition;

import static com.sdu.flink.runtime.network.NetworkConstants.TASK_MAX_PARALLELISM;
import static com.sdu.flink.runtime.network.NetworkConstants.UPSTREAM_RP_ID;
import static com.sdu.flink.runtime.network.NetworkConstants.UPSTREAM_TASK_NAME;
import static com.sdu.flink.runtime.network.NetworkConstants.UPSTREAM_TM_BIND_PORT;
import static org.apache.flink.configuration.NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_PER_CHANNEL;
import static org.apache.flink.configuration.NettyShuffleEnvironmentOptions.NETWORK_CREDIT_MODEL;
import static org.apache.flink.configuration.NettyShuffleEnvironmentOptions.NETWORK_EXCLUSIVE_BUFFERS_REQUEST_TIMEOUT_MILLISECONDS;
import static org.apache.flink.configuration.NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS;
import static org.apache.flink.configuration.TaskManagerOptions.MEMORY_SEGMENT_SIZE;
import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.PIPELINED_BOUNDED;

import com.sdu.flink.runtime.Bootstrap;
import com.sdu.flink.runtime.network.SendEndpointBufferPoolFactory;
import com.sdu.flink.runtime.taskexecutor.TaskManagerBootstrap;
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
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;

public class UpstreamTMTaskBootstrap implements Bootstrap {

  private TaskManagerBootstrap taskManagerBootstrap;
  private UpstreamStreamRecordEmitter emitter;

  @SuppressWarnings("unchecked")
  @Override
  public void setup(Configuration configuration) throws Exception {
    configuration.setInteger(NettyShuffleEnvironmentOptions.DATA_PORT, UPSTREAM_TM_BIND_PORT);
    configuration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 1);

    // 初始化TaskManager进程组件
    taskManagerBootstrap = new TaskManagerBootstrap();
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
    int networkBuffersPerChannel = configuration.getInteger(NETWORK_BUFFERS_PER_CHANNEL);
    int floatingNetworkBuffersPerGate = configuration.getInteger(NettyShuffleEnvironmentOptions.NETWORK_EXTRA_BUFFERS_PER_GATE);
    ResultPartitionType type = PIPELINED_BOUNDED;
    SendEndpointBufferPoolFactory bufferPoolFactory = new SendEndpointBufferPoolFactory(numberOfSubpartitions,
        networkBuffersPerChannel, floatingNetworkBuffersPerGate, type, taskManagerBootstrap.getNetworkBufferPool());
    ResultSubpartition[] subpartitions = new ResultSubpartition[1];
    ResultPartition resultPartition = new ResultPartition(UPSTREAM_TASK_NAME, UPSTREAM_RP_ID,
        type, subpartitions, TASK_MAX_PARALLELISM, taskManagerBootstrap.getResultPartitionManager(),
        bufferPoolFactory);
    // 构建下游每个Task的中间结果数据集
    for (int i = 0; i < subpartitions.length; ++i) {
      subpartitions[i] = new PipelinedSubpartitionWrapper(i, resultPartition);
    }
    resultPartition.setup();

    RecordWriter<SerializationDelegate<StreamElement>> writer = new RecordWriterBuilder()
        .setTaskName(UPSTREAM_TASK_NAME)
        .setChannelSelector(new ForwardPartitioner())
        .build(resultPartition);

    emitter = new UpstreamStreamRecordEmitter(writer);
  }

  @Override
  public void start() throws Exception {
    this.taskManagerBootstrap.start();
    this.emitter.start();
  }

  @Override
  public void stop() throws Exception {
    this.taskManagerBootstrap.stop();
    this.emitter.close();
  }

  public static void main(String[] args) throws Exception {
    Configuration configuration = new Configuration();
    // 设置缓冲区Buffer数量
    configuration.setInteger(NETWORK_NUM_BUFFERS, 128);
    // 设置Buffer容量
    configuration.setString(MEMORY_SEGMENT_SIZE, "16KB");
    // 每个Task可分配的Buffer数量
    configuration.setInteger(NETWORK_BUFFERS_PER_CHANNEL, 4);
    // 设置Buffer申请超时时间
    configuration.setLong(NETWORK_EXCLUSIVE_BUFFERS_REQUEST_TIMEOUT_MILLISECONDS, 5000L);
    // 基于credit
    configuration.setBoolean(NETWORK_CREDIT_MODEL, true);

    UpstreamTMTaskBootstrap upstreamTMTaskBootstrap = new UpstreamTMTaskBootstrap();
    upstreamTMTaskBootstrap.setup(configuration);
    upstreamTMTaskBootstrap.start();

    Runtime.getRuntime().addShutdownHook(new Thread() {

      @Override
      public void run() {
        try {
          upstreamTMTaskBootstrap.stop();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });

  }

}
