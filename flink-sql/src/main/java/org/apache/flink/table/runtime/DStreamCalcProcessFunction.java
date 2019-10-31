package org.apache.flink.table.runtime;

import static org.apache.flink.types.DSchemaType.CONDITION;
import static org.apache.flink.types.DSchemaType.PROJECT;
import static org.apache.flink.types.DSqlTypeUtils.objectToString;
import static org.apache.flink.types.DSqlTypeUtils.sqlTypeToJavaTypeAsString;

import com.google.gson.reflect.TypeToken;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.exec.DRexInvoker;
import org.apache.flink.types.DRecordTuple;
import org.apache.flink.types.DSchemaTuple;
import org.apache.flink.types.DSqlTypeUtils;
import org.apache.flink.types.DStreamRecord;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

public class DStreamCalcProcessFunction extends ProcessFunction<DStreamRecord, DStreamRecord> {

  private final String streamNodePath;
  private Map<String, DRexInvoker> projectFieldInvokers;

  private DRexInvoker conditionInvoker;

  public DStreamCalcProcessFunction(String streamNodePath, Map<String, DRexInvoker> projectFieldInvokers, DRexInvoker conditionInvoker) {
    Preconditions.checkNotNull(streamNodePath);
    Preconditions.checkNotNull(projectFieldInvokers);


    this.streamNodePath = streamNodePath;
    this.projectFieldInvokers = projectFieldInvokers;
    this.conditionInvoker = conditionInvoker;
  }



  @Override
  public void processElement(DStreamRecord streamRecord, Context ctx, Collector<DStreamRecord> out)
      throws Exception {

    switch (streamRecord.getRecordType()) {
      case RECORD:
        selectAndFilterStreamRecord(streamRecord, out);
        break;
      case SCHEMA:
        updateSchema(streamRecord, out);
        break;
      default:
        throw new IllegalArgumentException("Unsupported DStreamRecord, recordType: " + streamRecord.getRecordType());
    }

  }

  private void selectAndFilterStreamRecord(DStreamRecord streamRecord, Collector<DStreamRecord> out) {
    DRecordTuple recordTuple = streamRecord.recordTuple();

    assert conditionInvoker.getResultType() == SqlTypeName.BOOLEAN;
    // 过滤数据(Where条件)
    if (conditionInvoker != null && !((Boolean) conditionInvoker.invoke(recordTuple))) {
      System.err.println("Filter data: " + recordTuple);
      return;
    }

    // 映射字段
    Map<String, String> recordTypes = new HashMap<>();
    Map<String, String> recordValues = new HashMap<>();
    for (Entry<String, DRexInvoker> entry : projectFieldInvokers.entrySet()) {
      SqlTypeName resultType = entry.getValue().getResultType();

      String newFieldName = entry.getKey();
      String newFieldType = sqlTypeToJavaTypeAsString(resultType);
      recordTypes.put(newFieldName, newFieldType);

      Object value = entry.getValue().invoke(recordTuple);
      recordValues.put(newFieldName, objectToString(value, resultType));
    }

    out.collect(new DStreamRecord(new DRecordTuple(recordTypes, recordValues)));
  }

  private void updateSchema(DStreamRecord streamRecord, Collector<DStreamRecord> out) {
    DSchemaTuple schemaTuple = streamRecord.schemaTuple();
    if (schemaTuple == null) {
      return;
    }

    // 映射字段
    Map<String, DRexInvoker> fieldInvokers = schemaTuple.getStreamNodeSchema(streamNodePath, PROJECT,
        new TypeToken<Map<String, DRexInvoker>>(){}.getType());

    // 过滤条件
    DRexInvoker condition = schemaTuple.getStreamNodeSchema(streamNodePath, CONDITION, DRexInvoker.class);

    this.projectFieldInvokers = fieldInvokers;
    this.conditionInvoker = condition;

    // 向下游发送
    out.collect(streamRecord);
  }

}
