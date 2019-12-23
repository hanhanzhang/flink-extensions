package com.sdu.flink.runtime.network;

import java.io.IOException;
import java.util.Optional;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolOwner;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.util.function.FunctionWithException;

public class LocalBufferPoolFactory implements
    FunctionWithException<BufferPoolOwner, BufferPool, IOException> {

  private final int numRequiredBuffers;
  private final ResultPartitionType rpt;

  private final NetworkBufferPool networkBufferPool;

  public LocalBufferPoolFactory(int numRequiredBuffers, ResultPartitionType rpt, NetworkBufferPool networkBufferPool) {
    this.numRequiredBuffers = numRequiredBuffers;
    this.rpt = rpt;
    this.networkBufferPool = networkBufferPool;
  }

  @Override
  public BufferPool apply(BufferPoolOwner owner) throws IOException {
    return networkBufferPool.createBufferPool(numRequiredBuffers, numRequiredBuffers,
        rpt.hasBackPressure() ? Optional.empty() : Optional.of(owner));
  }


}
