package com.sdu.flink.network;

import static com.sdu.flink.network.BootstrapUtils.createNettyShuffleContext;
import static com.sdu.flink.network.BootstrapUtils.createNettyShuffleEnvironment;
import static com.sdu.flink.network.Constants.IDS;
import static com.sdu.flink.network.Constants.IRP;
import static com.sdu.flink.network.Constants.RP;
import static com.sdu.flink.network.Constants.TASK_EXECUTION_ATTEMPT_1;
import static com.sdu.flink.network.Constants.TASK_MANAGER_ID_1;
import static com.sdu.flink.network.Constants.TASK_MANAGER_PORT_1;
import static com.sdu.flink.network.Constants.TASK_MANAGER_PORT_2;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor.NetworkPartitionConnectionInfo;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleEnvironmentContext;
import org.apache.flink.runtime.shuffle.ShuffleIOOwnerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author hanhan.zhang
 * */
public class UpTaskManagerBootstrap {

  private static final Logger LOGGER = LoggerFactory.getLogger(UpTaskManagerBootstrap.class);

  public static void main(String[] args) throws Exception {

    InetAddress address = InetAddress.getLocalHost();

    ShuffleEnvironmentContext context = createNettyShuffleContext(TASK_MANAGER_ID_1, address, TASK_MANAGER_PORT_1);
    NettyShuffleEnvironment nettyShuffleEnvironment = createNettyShuffleEnvironment(context);
    int port = nettyShuffleEnvironment.start();
    LOGGER.info("TaskManager started on port {}, ", port);


    /*
     * 1: ShuffleIOOwnerContext
     *
     * 2: ResultPartitionDeploymentDescriptor
     *
     *    2.1 PartitionDescriptor
     *
     *        PartitionDescriptor描述Task数据输出信息
     *
     *    2.2 ShuffleDescriptor
     *
     *        ShuffleDescriptor描述下游Task连接信息
     * **/
    ShuffleIOOwnerContext ownerContext = nettyShuffleEnvironment.createShuffleIOOwnerContext(
        TASK_MANAGER_ID_1.toHexString(), TASK_EXECUTION_ATTEMPT_1, context.getParentMetricGroup());


    PartitionDescriptor partitionDescriptor = new PartitionDescriptor(IDS, IRP,
        ResultPartitionType.PIPELINED_BOUNDED, 1, 0);
    ConnectionID connectionID = new ConnectionID(new InetSocketAddress(address, TASK_MANAGER_PORT_2), 0);
    NettyShuffleDescriptor shuffleDescriptor = new NettyShuffleDescriptor(new ResourceID(TASK_MANAGER_ID_1.toString()),
        new NetworkPartitionConnectionInfo(connectionID), RP);
    ResultPartitionDeploymentDescriptor descriptor = new ResultPartitionDeploymentDescriptor(
        partitionDescriptor, shuffleDescriptor, 10, true);

    List<ResultPartitionDeploymentDescriptor> descriptors = Lists.newArrayList();
    descriptors.add(descriptor);

    Collection<ResultPartition> resultPartitions = nettyShuffleEnvironment.createResultPartitionWriters(
        ownerContext, descriptors);

    ResultPartition resultPartition = resultPartitions.iterator().next();

    /*
     * ResultPartition.setup()
     *
     * 1: 创建BufferPool(LocalBufferPool), BufferPoll向NetworkBufferPool申请MemorySegment
     *
     * 2: ResultPartition注册到ResultPartitionManager.
     * */
    resultPartition.setup();

    /*
     * 模拟 Task 向下游发送数据
     *
     * 1: ResultPartition.getBufferBuilder()
     *
     *   Task分配的MemorySegment由LocalBufferPool管理, 若是LocalBufferPool已申请的MemorySegment数量没有到阈值, 则可以向
     *
     *   NetworkBufferPool申请MemorySegment, 否则从LocalBufferPool返回可用MemorySegment.
     *
     *
     * 2: ResultPartition.addBufferConsumer()
     *
     *    SubPartitionResult缓存Task的数据结果, addBufferConsumer()负责将申请的MemorySegment添加到SubPartitionResult.
     *
     * 3: ResultPartition.flush()
     *
     *    flush()将SubPartitionResult缓存的数据结果发送到下游Task中.
     *
     * **/
    BufferBuilder bufferBuilder = resultPartition.getBufferBuilder();
    BufferConsumer bufferConsumer = bufferBuilder.createBufferConsumer();
    resultPartition.addBufferConsumer(bufferConsumer, 0);

    bufferBuilder.append(ByteBuffer.wrap("Hello world".getBytes()));

    resultPartition.flush(0);

    TimeUnit.HOURS.sleep(1);

  }

}
