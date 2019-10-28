package org.apache.flink.table;

import java.io.Serializable;

public interface DSqlUpdateListener extends Serializable {

  void notifySqlUpdate(String newSql);

}
