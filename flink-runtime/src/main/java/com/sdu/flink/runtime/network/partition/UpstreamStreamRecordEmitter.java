package com.sdu.flink.runtime.network.partition;

import static com.sdu.flink.utils.FakeDataUtils.buildAction;
import static com.sdu.flink.utils.FakeDataUtils.buildNameAndSex;
import static com.sdu.flink.utils.RandomUtils.nextInt;

import com.sdu.flink.entry.UserActionEntry;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;

public class UpstreamStreamRecordEmitter extends Thread {

  private final RecordWriter<SerializationDelegate<UserActionEntry>> writer;
  private volatile boolean running;

  UpstreamStreamRecordEmitter(RecordWriter<SerializationDelegate<UserActionEntry>> writer) {
    this.writer = writer;
    this.running = true;
  }

  @Override
  public void run() {
    TypeSerializer<UserActionEntry> serializer = Types.POJO(UserActionEntry.class)
        .createSerializer(new ExecutionConfig());
    SerializationDelegate<UserActionEntry> serializationDelegate = new SerializationDelegate<>(serializer);

    while (running) {
      try {
        UserActionEntry actionEntry = new UserActionEntry();
        actionEntry.setUid(nextInt(1000, 10000));
        Pair<String, String> nameAndSex = buildNameAndSex();
        actionEntry.setUname(nameAndSex.getLeft());
        actionEntry.setSex(nameAndSex.getRight());
        actionEntry.setAge(nextInt(10, 50));
        actionEntry.setAction(buildAction());
        actionEntry.setTimestamp(System.currentTimeMillis());

        serializationDelegate.setInstance(actionEntry);
        writer.emit(serializationDelegate);

        safeSleep();
      } catch (Exception e) {
        // ignore
      }
    }
  }

  void close() {
    this.running = false;
  }

  private static void safeSleep() {
    try {
      TimeUnit.MILLISECONDS.sleep(1000);
    } catch (Exception e) {
      // ignore
    }
  }

}
