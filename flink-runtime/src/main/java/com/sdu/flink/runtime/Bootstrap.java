package com.sdu.flink.runtime;


import java.net.InetAddress;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.util.ConfigurationParserUtils;

public interface Bootstrap {

  void setup(Configuration configuration) throws Exception ;

  void start() throws Exception;

  void stop() throws Exception;

  default NettyConfig fromConfiguration(Configuration configuration) throws Exception {
    InetAddress address = InetAddress.getLocalHost();
    int dataPort = configuration.getInteger(NettyShuffleEnvironmentOptions.DATA_PORT);
    int memorySegmentSize = ConfigurationParserUtils.getPageSize(configuration);
    int slotNum = ConfigurationParserUtils.getSlot(configuration);

    return new NettyConfig(address, dataPort, memorySegmentSize, slotNum, configuration);

  }

}
