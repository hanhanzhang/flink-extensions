package com.sdu.flink.entry;

import java.io.Serializable;
import lombok.Data;

@Data
public class CaptureActionEntry implements Serializable {

  private int version;
  private String action;

}
