package com.sdu.flink.runtime.taskexecutor;

import com.sdu.flink.runtime.Bootstrap;
import java.time.Duration;
import lombok.Data;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.netty.NettyConnectionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.util.ConfigurationParserUtils;

@Data
public class TaskManagerBootstrap implements Bootstrap {

  private NetworkBufferPool networkBufferPool;
  private ResultPartitionManager resultPartitionManager;
  private NettyConnectionManager connectionManager;

  @Override
  public void setup(Configuration configuration) throws Exception {
    /*
     * 1: NettyShuffleServiceFactory负责初始化TaskManager进程网络通信组件
     *
     * 2: NettyShuffleEnvironmentConfiguration.fromConfiguration()配置网络通信相关参数
     *
     *    2.1 Buffer Pool相关参数
     *
     *        2.1.1 NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS
     *
     *        2.1.2 NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_FRACTION
     *
     *        2.1.3 NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MIN
     *
     *        2.1.4 NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MAX
     *
     *        2.1.5 TaskManagerOptions.MEMORY_SEGMENT_SIZE
     *
     *    2.2 Buffer Pool Request相关参数
     *
     *        2.2.1 NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_PER_CHANNEL
     *
     *        2.2.2 NettyShuffleEnvironmentOptions.NETWORK_EXTRA_BUFFERS_PER_GATE
     *
     *        2.2.3 NettyShuffleEnvironmentOptions.NETWORK_EXCLUSIVE_BUFFERS_REQUEST_TIMEOUT_MILLISECONDS
     *
     *    2.3 Netty相关参数
     *
     *        2.3.1 NettyShuffleEnvironmentOptions.DATA_PORT
     *
     *        2.3.2 TaskManagerOptions.NUM_TASK_SLOTS
     *
     *    2.4 Backpressure相关参数
     *
     *        2.4.1 NettyShuffleEnvironmentOptions.NETWORK_CREDIT_MODEL
     * */

    // 这里使用NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS, 不考虑资源是否充足
    int numberOfNetworkBuffers = configuration.getInteger(NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS);
    int memorySegmentSize = ConfigurationParserUtils.getPageSize(configuration);
    int buffersPerChannel = configuration.getInteger(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_PER_CHANNEL);
    Duration requestSegmentsTimeout = Duration.ofMillis(configuration.getLong(
        NettyShuffleEnvironmentOptions.NETWORK_EXCLUSIVE_BUFFERS_REQUEST_TIMEOUT_MILLISECONDS));
    this.networkBufferPool = new NetworkBufferPool(numberOfNetworkBuffers,
        memorySegmentSize, buffersPerChannel, requestSegmentsTimeout);

    /*
     * ResultPartitionManager管理TM进程所有Task的数据输出结果集, ResultPartition.setup()实现向ResultPartitionManager注册
     * */
    this.resultPartitionManager = new ResultPartitionManager();

    boolean isCreditBased = configuration.getBoolean(NettyShuffleEnvironmentOptions.NETWORK_CREDIT_MODEL);
    this.connectionManager = new NettyConnectionManager(resultPartitionManager,
        new TaskEventDispatcher(), fromConfiguration(configuration), isCreditBased);

  }

  @Override
  public void start() throws Exception {
    // 启动NettyServer和初始化NettyClient
    this.connectionManager.start();
  }

  @Override
  public void stop() throws Exception {
    this.networkBufferPool.destroy();
    this.resultPartitionManager.shutdown();
    this.connectionManager.shutdown();
  }
}
