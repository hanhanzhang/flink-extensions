package org.apache.flink.table.updater;

import java.io.Serializable;
import java.util.List;
import lombok.Data;

@Data
public class DTableStatement implements Serializable {

  private String tableName;
  private List<DColumnStatement> columns;


}
