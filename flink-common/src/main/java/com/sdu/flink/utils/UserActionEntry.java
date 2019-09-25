package com.sdu.flink.utils;

import java.io.Serializable;

public class UserActionEntry implements Serializable {

  private String uid;

  private String action;

  private long timestamp;

  public UserActionEntry(String uid, String action, long timestamp) {
    this.uid = uid;
    this.action = action;
    this.timestamp = timestamp;
  }

  public String getUid() {
    return uid;
  }

  public String getAction() {
    return action;
  }

  public long getTimestamp() {
    return timestamp;
  }
}
