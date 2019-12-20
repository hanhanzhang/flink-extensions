package org.apache.flink.types;

import com.sdu.flink.utils.JsonUtils;
import java.io.Serializable;
import java.util.Objects;
import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

@Internal
public class DStreamRecord implements Serializable {

  private final DStreamRecordType recordType;

  private final String recordValue;

  public DStreamRecord(DSchemaTuple schemaTuple) {
    this(DStreamRecordType.SCHEMA, JsonUtils.toJson(schemaTuple));
  }

  public DStreamRecord(DRecordTuple recordTuple) {
    this(DStreamRecordType.RECORD, JsonUtils.toJson(recordTuple));
  }

  public DStreamRecord(DStreamRecordType recordType, String recordValue) {
    Preconditions.checkArgument(!StringUtils.isNullOrWhitespaceOnly(recordValue));

    this.recordType = recordType;
    this.recordValue = recordValue;
  }

  public DStreamRecordType getRecordType() {
    return recordType;
  }

  public DRecordTuple recordTuple() {
    if (recordType != DStreamRecordType.RECORD) {
      throw new RuntimeException("can't cast to DRecordTuple, recordType: " + recordType);
    }

    return JsonUtils.fromJson(recordValue, DRecordTuple.class);
  }

  public DSchemaTuple schemaTuple() {
    if (recordType != DStreamRecordType.SCHEMA) {
      throw new RuntimeException("can't cast to DSchemaTuple, recordType: " + recordType);
    }

    return JsonUtils.fromJson(recordValue, DSchemaTuple.class);
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DStreamRecord that = (DStreamRecord) o;
    return recordType == that.recordType &&
        recordValue.equals(that.recordValue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(recordType, recordValue);
  }


}
