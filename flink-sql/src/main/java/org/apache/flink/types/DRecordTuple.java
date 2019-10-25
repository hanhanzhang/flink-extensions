package org.apache.flink.types;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;
import lombok.Data;
import org.apache.flink.annotation.Internal;

@Internal
@Data
public class DRecordTuple implements Serializable {

  private final Map<String, String> recordTypes;
  private final Map<String, String> recordValues;

  public DRecordTuple(Map<String, String> recordTypes, Map<String, String> recordValues) {
    checkNotNull(recordTypes);
    checkNotNull(recordValues);
    checkState(recordTypes.size() == recordValues.size());

    this.recordTypes = recordTypes;
    this.recordValues = recordValues;
  }

  public String getRecordValue(String fieldName) {
    return recordValues.get(fieldName);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();

    boolean first = true;
    for (Entry<String, String> entry : recordTypes.entrySet()) {
      String recordName = entry.getKey();
      String recordType = entry.getValue();
      String recordValue = recordValues.get(recordName);

      if (first) {
        sb.append(recordName).append(": (").append(recordValue).append(", ").append(recordType).append(")");
        first = false;
      } else {
        sb.append(", ");
        sb.append(recordName).append(": (").append(recordValue).append(", ").append(recordType).append(")");
      }
    }

    return sb.toString();
  }
}
