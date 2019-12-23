package com.sdu.flink.runtime.network;

import java.net.InetAddress;
import java.net.UnknownHostException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.netty.NettyConfig;

public class NettyUtils {

  private NettyUtils() {}

  public static InetAddress getLocalInetAddress() throws UnknownHostException {
    return InetAddress.getLocalHost();
  }

  public static NettyConfig buildNettyConfig(
      InetAddress serverAddress, int serverPort, int memorySegmentSize, int numberOfSlots) {
    Configuration configuration = new Configuration();

    return new NettyConfig(serverAddress, serverPort, memorySegmentSize, numberOfSlots, configuration);
  }

}
