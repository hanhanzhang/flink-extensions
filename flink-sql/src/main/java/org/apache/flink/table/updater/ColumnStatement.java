package org.apache.flink.table.updater;

import java.io.Serializable;
import lombok.Data;

@Data
public class ColumnStatement implements Serializable {

  private String name;

  private String type;

  private String comment;

  public ColumnStatement(String name, String type) {
    this.name = name;
    this.type = type;
  }

}
