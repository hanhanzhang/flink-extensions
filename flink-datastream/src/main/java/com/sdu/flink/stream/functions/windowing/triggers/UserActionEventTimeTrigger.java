package com.sdu.flink.stream.functions.windowing.triggers;

import com.sdu.flink.entry.UserActionEntry;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * EventTimeTrigger对晚到的数据都会触发窗口计算, UserActionWindowTrigger对晚到数据不会触发窗口计算, 窗口计算只有两次:
 *
 * 1: 窗口的最大时间戳低于水位线时, 触发窗口计算
 *
 * 2: 窗口达到最大延迟的时间戳时, 触发窗口计算
 * */
public class UserActionEventTimeTrigger extends Trigger<UserActionEntry, TimeWindow> {

  private long lateness;

  public UserActionEventTimeTrigger(long lateness) {
    this.lateness = lateness;
  }

  @Override
  public TriggerResult onElement(UserActionEntry entry, long timestamp, TimeWindow window,
      TriggerContext ctx) throws Exception {
    // 处理数据流
    if (window.maxTimestamp() + lateness <= ctx.getCurrentWatermark()) {
      return TriggerResult.FIRE;
    } else {
      // 注册窗口触发时机(窗口的最大时间戳低于水位线时)
      ctx.registerEventTimeTimer(window.maxTimestamp());
      return TriggerResult.CONTINUE;
    }

  }

  @Override
  public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx)
      throws Exception {
    return TriggerResult.CONTINUE;
  }

  @Override
  public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx)
      throws Exception {
    return (time == window.maxTimestamp() || time == window.maxTimestamp() + lateness) ?
        TriggerResult.FIRE :
        TriggerResult.CONTINUE;
  }

  @Override
  public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
    ctx.deleteEventTimeTimer(window.maxTimestamp());
  }

}
