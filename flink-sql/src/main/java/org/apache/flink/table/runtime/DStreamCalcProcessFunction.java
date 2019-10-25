package org.apache.flink.table.runtime;

import static org.apache.flink.types.DTypeConverts.sqlTypeToJavaTypeAsString;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.codegen.DProjectFieldDExpression;
import org.apache.flink.types.DConditionSchema;
import org.apache.flink.types.DProjectSchemaData;
import org.apache.flink.types.DRecordTuple;
import org.apache.flink.types.DSchemaTuple;
import org.apache.flink.types.DStreamRecord;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

public class DStreamCalcProcessFunction extends ProcessFunction<DStreamRecord, DStreamRecord> {

  private List<DProjectFieldDExpression> projectFieldExpressions;

  public DStreamCalcProcessFunction(List<DProjectFieldDExpression> projectExpressions) {
    Preconditions.checkNotNull(projectExpressions);

    this.projectFieldExpressions = projectExpressions;
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
    // 过滤数据(Where条件)

    // 映射字段
    // TODO: DProjectFunctionExpression 起别名问题, 下游也应该跟着改
    Map<String, String> recordTypes = new HashMap<>();
    Map<String, String> recordValues = new HashMap<>();
    for (DProjectFieldDExpression expression : projectFieldExpressions) {
      String newFieldName = expression.getProjectFieldName();
      String newFieldType = sqlTypeToJavaTypeAsString(expression.getResultType());
      recordTypes.put(newFieldName, newFieldType);

      String value = expression.invoke(recordTuple);
      recordValues.put(newFieldName, value);
    }

    out.collect(new DStreamRecord(new DRecordTuple(recordTypes, recordValues)));
  }

  private void updateSchema(DStreamRecord streamRecord, Collector<DStreamRecord> out) {
    // 向下游发送变更Schema
    out.collect(streamRecord);
    /*
     * StreamCalc节点, 更新的Schema信息有两种:
     *
     * 1: 映射字段
     *
     * 2: 过滤条件
     * **/
    DSchemaTuple schemaTuple = streamRecord.schemaTuple();

    // 更新映射字段
    DProjectSchemaData projectSchema = schemaTuple.getProjectSchema();
    if (projectSchema != null) {
      // TODO: 2019-10-25 别名处理
    }

    // 更新过滤条件
    DConditionSchema conditionSchema = schemaTuple.getConditionSchema();
    if (conditionSchema != null) {
      // TODO: 2019-10-25
    }

  }

}
