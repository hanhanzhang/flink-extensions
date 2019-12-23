package com.sdu.flink.runtime.network;

import java.io.IOException;
import java.util.Optional;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolOwner;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.util.function.FunctionWithException;

/**
 * Sender endpoint
 *
 * @author hanhan.zhang
 * */
public class SendEndpointBufferPoolFactory implements
    FunctionWithException<BufferPoolOwner, BufferPool, IOException> {

  private final int numberOfSubpartitions;

  private final int networkBuffersPerChannel;
  private final int floatingNetworkBuffersPerGate;

  private final ResultPartitionType type;

  private final NetworkBufferPool networkBufferPool;

  public SendEndpointBufferPoolFactory(int numberOfSubpartitions, int networkBuffersPerChannel,
      int floatingNetworkBuffersPerGate, ResultPartitionType type, NetworkBufferPool networkBufferPool) {
    this.numberOfSubpartitions = numberOfSubpartitions;
    this.networkBuffersPerChannel = networkBuffersPerChannel;
    this.floatingNetworkBuffersPerGate = floatingNetworkBuffersPerGate;
    this.type = type;
    this.networkBufferPool = networkBufferPool;
  }

  @Override
  public BufferPool apply(BufferPoolOwner owner) throws IOException {
    int maxNumberOfMemorySegments = type.isBounded() ?
        numberOfSubpartitions * networkBuffersPerChannel + floatingNetworkBuffersPerGate : Integer.MAX_VALUE;
    return networkBufferPool.createBufferPool(numberOfSubpartitions, maxNumberOfMemorySegments,
        type.hasBackPressure() ? Optional.empty() : Optional.of(owner));
  }


}
