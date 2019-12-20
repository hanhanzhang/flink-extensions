package org.apache.flink.table.updater;

import java.io.Serializable;
import lombok.Data;

@Data
public class DColumnStatement implements Serializable {

  private String name;

  private String type;

  private String comment;

  public DColumnStatement(String name, String type) {
    this.name = name;
    this.type = type;
  }

}
