package com.sdu.flink.runtime.network.partition;

import static com.sdu.flink.runtime.network.NettyUtils.buildNettyConfig;
import static com.sdu.flink.runtime.network.NettyUtils.getLocalInetAddress;
import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.PIPELINED;

import com.sdu.flink.entry.UserActionEntry;
import com.sdu.flink.runtime.network.LocalBufferPoolFactory;
import com.sdu.flink.runtime.network.NetworkConstants;
import com.sdu.flink.runtime.network.TaskEventPublisherImpl;
import com.sdu.flink.runtime.network.UserActionEmitter;
import java.net.InetAddress;
import java.time.Duration;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterBuilder;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.netty.NettyConnectionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;

public class UpstreamTMBootstrap {

  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {

    InetAddress serverAddress = getLocalInetAddress();
    int serverPort = 6715;
    int memorySegmentSize = (int) MemorySize.parse("32KB").getBytes();
    int numberOfSlots = 1;


    /*
     * 1 TaskManager进程组件
     *
     *    1.1 ResultPartitionManager
     *
     *    1.2 NettyConnectionManager
     *
     *    1.3 NetworkBufferPool
     *
     *
     * 2 Task线程组件
     *
     *    2.1 RecordWriter
     *
     *    2.2 ResultPartition
     *
     * */

    // TM总共申请1024个Buffer, 每个Buffer容量为32KB, 每个Channel分配16个Buffer
    NetworkBufferPool networkBufferPool = new NetworkBufferPool(1024,
        (int) MemorySize.parse("32KB").getBytes(), 16, Duration.ofSeconds(5));
    ResultPartitionManager partitionManager = new ResultPartitionManager();
    NettyConnectionManager connectionManager = new NettyConnectionManager(partitionManager,
        new TaskEventPublisherImpl(), buildNettyConfig(serverAddress, serverPort, memorySegmentSize, numberOfSlots), true);
    /*
     * 1: TM进程启动NettyServer, 负责处理上下游Task发送的消息
     *
     * 2: TM进程初始化NettyClient(但未创建链接)配置
     * */
    connectionManager.start();

    /*
     *                           +------------+
     *                    +----->| Task1(1/2) |
     *                    |      +------------+
     * +------------+     |
     * | Task0(1/2) |-----+
     * +------------+     |
     *                    |      +------------+
     *                    +----> | Task1(2/2) |
     *                           +------------+
     *
     * 向下游两个Task发送数据, 每个Task分配16个Buffer, 需申请2 * 16个buffer
     * */
    int numRequiredBuffers = 2 * 16;
    LocalBufferPoolFactory bufferPoolFactory = new LocalBufferPoolFactory(numRequiredBuffers, PIPELINED, networkBufferPool);
    ResultPartition resultPartition = new ResultPartition("Task0(1/2)", NetworkConstants.RP,
        PIPELINED, new ResultSubpartition[2], 128, partitionManager, bufferPoolFactory);
    resultPartition.setup();

    KeySelector<UserActionEntry, Integer> selector = UserActionEntry::getUid;
    RecordWriter<SerializationDelegate<UserActionEntry>> writer = new RecordWriterBuilder()
        .setTaskName("Task0(1/2)")
        .setChannelSelector(new KeyGroupStreamPartitioner(selector, 128))
        .build(resultPartition);

    // 写数据线程
    final UserActionEmitter emitter = new UserActionEmitter(writer);
    emitter.start();

    Runtime.getRuntime().addShutdownHook(new Thread(() -> emitter.close()));
  }

}
