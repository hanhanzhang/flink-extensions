package com.sdu.flink.runtime.network.partition;

import com.sdu.flink.entry.UserActionEntry;
import org.apache.flink.streaming.runtime.io.StreamTaskNetworkInput;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DownstreamStreamRecordConsumer extends Thread {

  private static final Logger LOG = LoggerFactory.getLogger(DownstreamStreamRecordConsumer.class);

  private StreamTaskNetworkInput taskNetworkInput;
  private volatile boolean running;

  DownstreamStreamRecordConsumer(StreamTaskNetworkInput taskNetworkInput) {
    this.taskNetworkInput = taskNetworkInput;
    this.running = true;
  }

  @Override
  public void run() {
    try {
      while (running) {
        StreamElement element = taskNetworkInput.pollNextNullable();
        if (element != null && element.isRecord()) {
          StreamRecord<UserActionEntry> record = element.asRecord();
          UserActionEntry actionEntry = record.getValue();

          LOG.info("Received upstream record, uid: {}, name: {}, action: {}, timestamp:{}",
              actionEntry.getUid(), actionEntry.getUname(), actionEntry.getAction(), actionEntry.getTimestamp());
        }
      }
    } catch (Exception e) {
      // ignore
    }
  }

  public void close() {
    this.running = false;
  }

}
