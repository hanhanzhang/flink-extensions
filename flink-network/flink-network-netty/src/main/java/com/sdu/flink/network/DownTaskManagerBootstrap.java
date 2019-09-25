package com.sdu.flink.network;

import static com.sdu.flink.network.BootstrapUtils.createNettyShuffleContext;
import static com.sdu.flink.network.BootstrapUtils.createNettyShuffleEnvironment;
import static com.sdu.flink.network.Constants.IDS;
import static com.sdu.flink.network.Constants.RP;
import static com.sdu.flink.network.Constants.TASK_EXECUTION_ATTEMPT_2;
import static com.sdu.flink.network.Constants.TASK_MANAGER_ID_1;
import static com.sdu.flink.network.Constants.TASK_MANAGER_ID_2;
import static com.sdu.flink.network.Constants.TASK_MANAGER_PORT_1;
import static com.sdu.flink.network.Constants.TASK_MANAGER_PORT_2;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor.NetworkPartitionConnectionInfo;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleEnvironmentContext;
import org.apache.flink.runtime.shuffle.ShuffleIOOwnerContext;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author hanhan.zhang
 * */
public class DownTaskManagerBootstrap {

  private static final Logger LOGGER = LoggerFactory.getLogger(DownTaskManagerBootstrap.class);

  public static void main(String[] args) throws Exception {

    InetAddress address = InetAddress.getLocalHost();

    ShuffleEnvironmentContext context = createNettyShuffleContext(TASK_MANAGER_ID_2, address, TASK_MANAGER_PORT_2);
    NettyShuffleEnvironment nettyShuffleEnvironment = createNettyShuffleEnvironment(context);
    int port = nettyShuffleEnvironment.start();
    LOGGER.info("TaskManager started on port {}, ", port);

    /*
     *
     *
     * **/
    ShuffleIOOwnerContext ownerContext = nettyShuffleEnvironment.createShuffleIOOwnerContext(
        TASK_MANAGER_ID_2.toHexString(), TASK_EXECUTION_ATTEMPT_2, context.getParentMetricGroup());

    PartitionProducerStateProvider NO_OP_PRODUCER_CHECKER = (sid, id, consume) -> {};

    List<InputGateDeploymentDescriptor> inputGateDeployDescriptors = Lists.newArrayList();
    // TaskManager01连接地址
    ConnectionID connectionID = new ConnectionID(new InetSocketAddress(address, TASK_MANAGER_PORT_1), 0);
    NettyShuffleDescriptor inputShuffleDescriptor = new NettyShuffleDescriptor(
        new ResourceID(TASK_MANAGER_ID_1.toString()),
        new NetworkPartitionConnectionInfo(connectionID), RP);
    InputGateDeploymentDescriptor inputDescriptor = new InputGateDeploymentDescriptor(
        IDS, ResultPartitionType.PIPELINED_BOUNDED, 0,  new ShuffleDescriptor[] {inputShuffleDescriptor});
    inputGateDeployDescriptors.add(inputDescriptor);

    /*
     * SingleInputGateFactory.create()
     *
     * 1: 构建BufferFactory(底层NetworkBufferFactory), BufferFactory构建LocalBufferPool
     *
     * 2: 构建InputChannel(根据NettyShuffleDescriptor构建RemoteInputChannel)
     * **/
    Collection<SingleInputGate> inputGates = nettyShuffleEnvironment.createInputGates(ownerContext, NO_OP_PRODUCER_CHECKER, inputGateDeployDescriptors);
    InputGate inputGate = inputGates.iterator().next();

    /*
     * InputGate.setup()
     *
     * 1: 申请数据接收Buffer
     *
     * 2: 向上游Task发送PartitionRequest请求, 上游Task接收PartitionRequest后, 创建ResultPartition/SubResultPartition数据
     *
     *    ResultSubpartitionView.
     *
     * **/
    inputGate.setup();

    Optional<BufferOrEvent> bufferOrEvent = inputGate.getNext();

    if (bufferOrEvent.isPresent()) {
      BufferOrEvent be = bufferOrEvent.get();
      assert be.isBuffer();
      ByteBuf buffer = be.getBuffer().asByteBuf();
      byte[] bytes = new byte[buffer.readableBytes()];
      buffer.readBytes(bytes);
      LOGGER.info("Received message: {}", new String(bytes));
    }

    TimeUnit.HOURS.sleep(1);
  }

}
