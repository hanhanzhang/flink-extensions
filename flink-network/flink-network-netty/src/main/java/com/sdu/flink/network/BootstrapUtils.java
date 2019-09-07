package com.sdu.flink.network;

import static org.apache.flink.configuration.MetricOptions.SCOPE_DELIMITER;
import static org.apache.flink.configuration.MetricOptions.SCOPE_NAMING_JM;
import static org.apache.flink.configuration.MetricOptions.SCOPE_NAMING_JM_JOB;
import static org.apache.flink.configuration.MetricOptions.SCOPE_NAMING_OPERATOR;
import static org.apache.flink.configuration.MetricOptions.SCOPE_NAMING_TASK;
import static org.apache.flink.configuration.MetricOptions.SCOPE_NAMING_TM;
import static org.apache.flink.configuration.MetricOptions.SCOPE_NAMING_TM_JOB;
import static org.apache.flink.configuration.NettyShuffleEnvironmentOptions.DATA_PORT;
import static org.apache.flink.configuration.NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_FRACTION;
import static org.apache.flink.configuration.NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MAX;
import static org.apache.flink.configuration.NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MIN;
import static org.apache.flink.configuration.NettyShuffleEnvironmentOptions.NUM_THREADS_CLIENT;
import static org.apache.flink.configuration.NettyShuffleEnvironmentOptions.TRANSPORT_TYPE;
import static org.apache.flink.configuration.TaskManagerOptions.MANAGED_MEMORY_SIZE;
import static org.apache.flink.configuration.TaskManagerOptions.MEMORY_OFF_HEAP;
import static org.apache.flink.configuration.TaskManagerOptions.MEMORY_SEGMENT_SIZE;

import java.net.InetAddress;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.NettyShuffleServiceFactory;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.shuffle.ShuffleEnvironmentContext;
import org.apache.flink.util.AbstractID;

/**
 * @author hanhan.zhang
 * */
class BootstrapUtils {

  static ShuffleEnvironmentContext createNettyShuffleContext(AbstractID taskMangerId, InetAddress address, int port) {
    Configuration config = new Configuration();

    /*
     * 1: Metric Config
     *
     * 2: Memory Config
     *
     * 3: Network Config
     * **/
    config.setString(SCOPE_NAMING_JM, "<host>.jm");
    config.setString(SCOPE_NAMING_JM_JOB, "<host>.jm.<job_name>");
    config.setString(SCOPE_NAMING_TM, "<host>.tm");
    config.setString(SCOPE_NAMING_TM_JOB, "<host>.tm.<tm_id>.<job_name>");
    config.setString(SCOPE_NAMING_TASK, "<host>.tm.<tm_id>.<job_name>.<task_name>.<task_index>");
    config.setString(SCOPE_NAMING_OPERATOR, "<host>.tm.<tm_id>.<job_name>.<operator_name>.<subtask_index>");
    config.setString(SCOPE_DELIMITER, ".");

    config.setBoolean(MEMORY_OFF_HEAP, true);

    config.setFloat(NETWORK_BUFFERS_MEMORY_FRACTION, 0.5f);
    // network buffer = 1024m
    // memory_segment = 32m
    // buffer_numbers = 32
    config.setString(MANAGED_MEMORY_SIZE, "1024m");
    config.setString(MEMORY_SEGMENT_SIZE, "32m");
    config.setString(NETWORK_BUFFERS_MEMORY_MIN, "512m");
    config.setString(NETWORK_BUFFERS_MEMORY_MAX, "1024m");

    // IO模型: nio, epoll
    config.setString(TRANSPORT_TYPE, "nio");
    // 设置客户端工作线程数, 默认值: slotNumbers
    config.setInteger(NUM_THREADS_CLIENT, Runtime.getRuntime().availableProcessors());
    config.setInteger(DATA_PORT, port);

    /*
     * 1: MetricRegistryImpl
     *
     * 2: TaskManagerMetricGroup
     *
     * **/
    MetricRegistryConfiguration metricRegistryConfig = MetricRegistryConfiguration.fromConfiguration(config);
    MetricRegistry metricRegistry = new MetricRegistryImpl(metricRegistryConfig);
    MetricGroup parentMetricGroup = new TaskManagerMetricGroup(metricRegistry, address.getHostAddress(),
        taskMangerId.toHexString());

    final long maxJvmHeapMemory = 2 * 1024 * 1024 * 1024L;

    TaskEventDispatcher eventPublisher = new TaskEventDispatcher();

    return new ShuffleEnvironmentContext(config,
        new ResourceID(taskMangerId.toHexString()), maxJvmHeapMemory, false, address, eventPublisher, parentMetricGroup);
  }

  static NettyShuffleEnvironment createNettyShuffleEnvironment(ShuffleEnvironmentContext context) {
    /*
     * NettyShuffleServiceFactory 初始化网络数据传输的相关组件(TaskManager进程中的所有Task共享, 单例)
     *
     * 1: ResultPartitionManager
     *
     *    ResultPartitionManager管理ResultPartition和数据消费.
     *
     * 2: ConnectionManager
     *
     *    ConnectionManager负责网络数据读/写, 其主要属性
     *
     *    2.1 NettyServer
     *
     *       接收上游Task发送过来的数据
     *
     *    2.2 PartitionRequestClientFactory
     *
     *       创建NettyClient, NettyClient向下游发送数据.
     *
     *    2.3 NettyProtocol
     *
     *       NettyProtocol定义NettyServer和NettyClient的事件处理器(ChannelHandler)
     *
     * 3: NetworkBufferPool
     *
     *    Network MemorySegment 资源池
     *
     * 4: ResultPartitionFactory
     *
     *    ResultPartitionFactory 用来创建 ResultPartition, ResultPartition 缓存Task数据结果(内部有FlushThread触发数据结果
     *
     *    发送到下游).
     *
     * 5: SingleInputGateFactory
     *
     *    SingleInputGateFactory 用来创建 SingleInputGate, SingleInputGate 读取上游Task发送过来的数据.
     *
     *
     * **/
    NettyShuffleServiceFactory nettyShuffleServiceFactory = new NettyShuffleServiceFactory();
    return nettyShuffleServiceFactory.createShuffleEnvironment(context);
  }

}
