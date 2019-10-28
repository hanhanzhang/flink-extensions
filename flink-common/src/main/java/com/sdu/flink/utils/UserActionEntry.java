package com.sdu.flink.utils;

import java.io.Serializable;

public class UserActionEntry implements Serializable {

  private String uid;

  private int age;

  private boolean isForeigners;

  private String action;

  private long timestamp;

  public UserActionEntry(String uid, int age, boolean isForeigners,  String action, long timestamp) {
    this.uid = uid;
    this.age = age;
    this.isForeigners = isForeigners;
    this.action = action;
    this.timestamp = timestamp;
  }

  public String getUid() {
    return uid;
  }

  public int getAge() {
    return age;
  }

  public boolean isForeigners() {
    return isForeigners;
  }

  public String getAction() {
    return action;
  }

  public long getTimestamp() {
    return timestamp;
  }


}
