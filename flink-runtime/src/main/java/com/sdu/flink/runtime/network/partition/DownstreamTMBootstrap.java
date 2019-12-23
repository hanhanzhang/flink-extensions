package com.sdu.flink.runtime.network.partition;

import com.sdu.flink.runtime.Bootstrap;
import org.apache.flink.configuration.Configuration;

public class DownstreamTMBootstrap implements Bootstrap {

  @Override
  public void setup(Configuration configuration) throws Exception {
    /*
     * Task组件
     *
     * 1: InputGate
     *
     *    NettyShuffleEnvironment.createInputGates()构建InputGate, InputGate负责消费上游Task生产的数据
     *
     * 2:
     * */

  }

  @Override
  public void start() throws Exception {

  }

  @Override
  public void stop() throws Exception {

  }

  public static void main(String[] args) {

  }
}
