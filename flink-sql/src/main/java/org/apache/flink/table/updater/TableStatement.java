package org.apache.flink.table.updater;

import java.io.Serializable;
import java.util.List;
import lombok.Data;

@Data
public class TableStatement implements Serializable {

  private String tableName;
  private List<ColumnStatement> columns;


}
