package com.sdu.flink.stream.functions;

import com.sdu.flink.entry.UserActionEntry;
import javax.annotation.Nullable;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

public class UserActionStreamAssignWatermarks implements AssignerWithPeriodicWatermarks<UserActionEntry> {

  private long currentMaxTimestamp = Long.MIN_VALUE;

  @Nullable
  @Override
  public Watermark getCurrentWatermark() {
    return new Watermark(currentMaxTimestamp);
  }

  @Override
  public long extractTimestamp(UserActionEntry element, long previousElementTimestamp) {
    long elementTimestamp = element.getTimestamp();
    if (elementTimestamp > currentMaxTimestamp) {
      currentMaxTimestamp = element.getTimestamp();
    }
    return elementTimestamp;
  }

}
