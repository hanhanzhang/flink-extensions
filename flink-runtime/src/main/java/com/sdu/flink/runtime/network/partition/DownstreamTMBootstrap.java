package com.sdu.flink.runtime.network.partition;

import static com.sdu.flink.runtime.network.NetworkConstants.DOWNSTREAM_TASK_NAME;
import static com.sdu.flink.runtime.network.NetworkConstants.DOWNSTREAM_TM_BIND_PORT;
import static com.sdu.flink.runtime.network.NetworkConstants.UPSTREAM_IDS_ID;
import static com.sdu.flink.runtime.network.NetworkConstants.UPSTREAM_RP_ID;
import static com.sdu.flink.runtime.network.NetworkConstants.UPSTREAM_TM_BIND_PORT;
import static org.apache.flink.configuration.NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_PER_CHANNEL;
import static org.apache.flink.configuration.NettyShuffleEnvironmentOptions.NETWORK_CREDIT_MODEL;
import static org.apache.flink.configuration.NettyShuffleEnvironmentOptions.NETWORK_EXCLUSIVE_BUFFERS_REQUEST_TIMEOUT_MILLISECONDS;
import static org.apache.flink.configuration.NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS;
import static org.apache.flink.configuration.TaskManagerOptions.MEMORY_SEGMENT_SIZE;
import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.PIPELINED_BOUNDED;

import com.sdu.flink.entry.UserActionEntry;
import com.sdu.flink.runtime.Bootstrap;
import com.sdu.flink.runtime.network.ReceiveEndpointBufferPoolFactory;
import com.sdu.flink.runtime.taskexecutor.TaskManagerBootstrap;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.metrics.InputChannelMetrics;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.scope.ScopeFormats;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.runtime.io.CheckpointedInputGate;
import org.apache.flink.streaming.runtime.io.InputProcessorUtil;
import org.apache.flink.streaming.runtime.io.StreamTaskNetworkInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DownstreamTMBootstrap implements Bootstrap {

  private static final Logger LOG = LoggerFactory.getLogger(DownstreamTMBootstrap.class);

  private TaskManagerBootstrap taskManagerBootstrap;
  private SingleInputGate inputGate;
  private StreamTaskNetworkInput taskNetworkInput;
  private DownstreamStreamRecordConsumer streamRecordConsumer;

  private MetricGroup createParentMetricGroup(Configuration configuration) throws Exception {
    ScopeFormats scopeFormats = ScopeFormats.fromConfig(configuration);
    char delim;
    try {
      delim = configuration.getString(MetricOptions.SCOPE_DELIMITER).charAt(0);
    } catch (Exception e) {
      LOG.warn("Failed to parse delimiter, using default delimiter.", e);
      delim = '.';
    }
    MetricRegistryConfiguration registryConfiguration = new MetricRegistryConfiguration(scopeFormats, delim, 10);

    MetricRegistry metricRegistry = new MetricRegistryImpl(registryConfiguration);
    return new TaskManagerMetricGroup(metricRegistry, InetAddress.getLocalHost().getHostName(),
        UUID.randomUUID().toString());
  }

  @Override
  public void setup(Configuration configuration) throws Exception {
    configuration.setInteger(NettyShuffleEnvironmentOptions.DATA_PORT, DOWNSTREAM_TM_BIND_PORT);
    configuration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 1);
    // 初始化TaskManager进程组件
    taskManagerBootstrap = new TaskManagerBootstrap();
    taskManagerBootstrap.setup(configuration);

    InetSocketAddress upstreamTMAddress = new InetSocketAddress(InetAddress.getLocalHost(), UPSTREAM_TM_BIND_PORT);
    ConnectionID connectionID = new ConnectionID(upstreamTMAddress, 1);

    /*
     * Task组件
     *
     * 1: InputGate
     *
     *    NettyShuffleEnvironment.createInputGates()构建InputGate, InputGate负责消费上游Task生产的数据
     *
     * 2: InputChannel
     *
     *    InputChannel存储上游Task拉取的数据
     *
     * 3: StreamTaskNetworkInput
     *
     *    StreamTaskNetworkInput负责从InputChannel读取数据, 然后由StreamOperator处理收到的消息
     * */
    int networkBuffersPerChannel = configuration.getInteger(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_PER_CHANNEL);
    int floatingNetworkBuffersPerGate = configuration.getInteger(NettyShuffleEnvironmentOptions.NETWORK_EXTRA_BUFFERS_PER_GATE);
    boolean isCreditBased = configuration.getBoolean(NettyShuffleEnvironmentOptions.NETWORK_CREDIT_MODEL);
    ResultPartitionType type = PIPELINED_BOUNDED;
    int initialRequestBackoff = configuration.getInteger(NettyShuffleEnvironmentOptions.NETWORK_REQUEST_BACKOFF_INITIAL);
    int maxRequestBackoff = configuration.getInteger(NettyShuffleEnvironmentOptions.NETWORK_REQUEST_BACKOFF_MAX);
    InputChannelMetrics inputChannelMetrics = new InputChannelMetrics(createParentMetricGroup(configuration));

    // Buffer申请
    ReceiveEndpointBufferPoolFactory bufferPoolFactory = new ReceiveEndpointBufferPoolFactory(1,
        networkBuffersPerChannel, floatingNetworkBuffersPerGate, taskManagerBootstrap.getNetworkBufferPool(), type, isCreditBased);

    inputGate =  new SingleInputGate(DOWNSTREAM_TASK_NAME, UPSTREAM_IDS_ID, type, 0, 1,
        new MockTask(), isCreditBased, bufferPoolFactory);

    InputChannel inputChannel = new RemoteInputChannel(
        inputGate, 0, UPSTREAM_RP_ID, connectionID, taskManagerBootstrap.getConnectionManager(), initialRequestBackoff,
        maxRequestBackoff, inputChannelMetrics, taskManagerBootstrap.getNetworkBufferPool());
    ResultPartitionID resultPartitionID = inputChannel.getPartitionId();
    inputGate.setInputChannel(resultPartitionID.getPartitionId(), inputChannel);

    //
    TypeSerializer<UserActionEntry> serializer = Types.POJO(UserActionEntry.class)
        .createSerializer(new ExecutionConfig());
    IOManager ioManager = new IOManagerAsync();
    CheckpointedInputGate checkpointedInputGate = InputProcessorUtil.createCheckpointedInputGate(null, CheckpointingMode.AT_LEAST_ONCE,
        ioManager, inputGate, configuration, DOWNSTREAM_TASK_NAME);
    taskNetworkInput = new StreamTaskNetworkInput(checkpointedInputGate, serializer, ioManager, 0);
    streamRecordConsumer = new DownstreamStreamRecordConsumer(taskNetworkInput);
  }

  @Override
  public void start() throws Exception {
    // 启动NettyServer, 初始化NettyClient
    taskManagerBootstrap.start();
    /*
     * 订阅上游Task数据结果集
     * */
    inputGate.setup();
    streamRecordConsumer.start();
  }

  @Override
  public void stop() throws Exception {
    taskManagerBootstrap.stop();
    inputGate.close();
    taskNetworkInput.close();
    streamRecordConsumer.close();
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

    DownstreamTMBootstrap downstreamTMBootstrap = new DownstreamTMBootstrap();
    downstreamTMBootstrap.setup(configuration);
    downstreamTMBootstrap.start();

    TimeUnit.HOURS.sleep(1);

    Runtime.getRuntime().addShutdownHook(new Thread() {

      @Override
      public void run() {
        try {
          downstreamTMBootstrap.stop();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
  }
}
