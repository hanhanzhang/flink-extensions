package com.sdu.flink.utils;

import java.io.Serializable;
import lombok.Data;

@Data
public class UserActionEntry implements Serializable {

  private String uid;
  private String uname;
  private String sex;
  private int age;
  private String action;
  private long timestamp;

}
