package com.sdu.flink.runtime.network;

import java.io.IOException;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolFactory;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.util.function.SupplierWithException;

public class ReceiveEndpointBufferPoolFactory implements SupplierWithException<BufferPool, IOException> {

  private final int size;
  private final int networkBuffersPerChannel;
  private final int floatingNetworkBuffersPerGate;

  private final BufferPoolFactory bufferPoolFactory;
  private final ResultPartitionType type;

  private final boolean isCreditBased;

  public ReceiveEndpointBufferPoolFactory(int size, int networkBuffersPerChannel, int floatingNetworkBuffersPerGate,
      BufferPoolFactory bufferPoolFactory, ResultPartitionType type, boolean isCreditBased) {
    this.size = size;
    this.networkBuffersPerChannel = networkBuffersPerChannel;
    this.floatingNetworkBuffersPerGate = floatingNetworkBuffersPerGate;
    this.bufferPoolFactory = bufferPoolFactory;
    this.type = type;
    this.isCreditBased = isCreditBased;
  }

  @Override
  public BufferPool get() throws IOException {
    if (isCreditBased) {
      int maxNumberOfMemorySegments = type.isBounded() ? floatingNetworkBuffersPerGate : Integer.MAX_VALUE;
      return bufferPoolFactory.createBufferPool(0, maxNumberOfMemorySegments);
    } else {
      int maxNumberOfMemorySegments = type.isBounded() ?
          size * networkBuffersPerChannel + floatingNetworkBuffersPerGate : Integer.MAX_VALUE;
      return bufferPoolFactory.createBufferPool(size, maxNumberOfMemorySegments);
    }
  }

}
