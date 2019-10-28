package org.apache.flink.table.runtime;

import static org.apache.flink.types.DTypeConverts.javaTypeToSqlType;
import static org.apache.flink.types.DTypeConverts.sqlTypeToJavaTypeAsString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.codegen.DRexInputRefInvoker;
import org.apache.flink.table.codegen.DRexInvoker;
import org.apache.flink.table.codegen.DSqlFunctionInvoker;
import org.apache.flink.types.DConditionSchema;
import org.apache.flink.types.DFuncProjectFieldInfo;
import org.apache.flink.types.DProjectFieldInfo;
import org.apache.flink.types.DProjectSchema;
import org.apache.flink.types.DRecordTuple;
import org.apache.flink.types.DSchemaTuple;
import org.apache.flink.types.DStreamRecord;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

public class DStreamCalcProcessFunction extends ProcessFunction<DStreamRecord, DStreamRecord> {

  private Map<String, DRexInvoker<?>> projectFieldInvokers;

  private DRexInvoker<Boolean> conditionInvoker;

  public DStreamCalcProcessFunction(Map<String, DRexInvoker<?>> projectFieldInvokers, DRexInvoker<Boolean> conditionInvoker) {
    Preconditions.checkNotNull(projectFieldInvokers);

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
    // 过滤数据(Where条件)
    if (conditionInvoker != null && !conditionInvoker.invoke(recordTuple)) {
      System.err.println("Filter data: " + recordTuple);
      return;
    }

    // 映射字段
    Map<String, String> recordTypes = new HashMap<>();
    Map<String, String> recordValues = new HashMap<>();
    for (Entry<String, DRexInvoker<?>> entry : projectFieldInvokers.entrySet()) {
      String newFieldName = entry.getKey();
      String newFieldType = sqlTypeToJavaTypeAsString(entry.getValue().getResultType());
      recordTypes.put(newFieldName, newFieldType);

      // TODO: 2019-10-28 类型处理
      Object value = entry.getValue().invoke(recordTuple);
      recordValues.put(newFieldName, String.valueOf(value));
    }

    out.collect(new DStreamRecord(new DRecordTuple(recordTypes, recordValues)));
  }

  private void updateSchema(DStreamRecord streamRecord, Collector<DStreamRecord> out) {
    /*
     * StreamCalc节点, 更新的Schema信息有两种:
     *
     * 1: 映射字段
     *
     * 2: 过滤条件
     * **/
    DSchemaTuple schemaTuple = streamRecord.schemaTuple();

    DProjectSchema projectSchema = schemaTuple.getProjectSchema();
    if (projectSchema != null) {
      Map<String, DProjectFieldInfo> inputProjectFields = projectSchema.getInputProjectFields();
      if (MapUtils.isNotEmpty(inputProjectFields)) {
        Map<String, DRexInvoker<?>> projectFieldInvokers = new HashMap<>();

        for (Entry<String, DProjectFieldInfo> entry : inputProjectFields.entrySet()) {
          // outputFieldName 可能是别名, 如 udf(field_name) as new_name
          String outputFieldName = entry.getKey();
          DProjectFieldInfo projectFieldInfo = entry.getValue();

          //
          if (projectFieldInfo instanceof DFuncProjectFieldInfo) {
            DFuncProjectFieldInfo funcProjectFieldInfo = (DFuncProjectFieldInfo) projectFieldInfo;
            List<DRexInvoker<?>> parameterRexInvokes = fromFuncProjectFieldInfo(funcProjectFieldInfo);
            DSqlFunctionInvoker sqlFunctionInvoker = new DSqlFunctionInvoker(funcProjectFieldInfo.getClassName(),
                parameterRexInvokes, javaTypeToSqlType(funcProjectFieldInfo.getFieldType()));
            projectFieldInvokers.put(outputFieldName, sqlFunctionInvoker);

          } else {
            DRexInputRefInvoker inputRefInvoker = new DRexInputRefInvoker(projectFieldInfo.getFieldName(),
                javaTypeToSqlType(projectFieldInfo.getFieldType()));
            projectFieldInvokers.put(outputFieldName, inputRefInvoker);
          }
        }

        this.projectFieldInvokers = projectFieldInvokers;
      }
    }

    // 更新过滤条件
    DConditionSchema conditionSchema = schemaTuple.getConditionSchema();
    if (conditionSchema != null) {
      // TODO: 2019-10-25
    }

    // 向下游发送变更Schema
    out.collect(streamRecord);
  }

  private static List<DRexInvoker<?>> fromFuncProjectFieldInfo(DFuncProjectFieldInfo funcProjectFieldInfo) {
    List<DProjectFieldInfo> projectFieldInfos = funcProjectFieldInfo.getParameterProjectFields();
    if (CollectionUtils.isEmpty(projectFieldInfos)) {
      return Collections.emptyList();
    }

    List<DRexInvoker<?>> rexInvokers = new ArrayList<>();
    for (DProjectFieldInfo projectFieldInfo : projectFieldInfos) {
      if (projectFieldInfo instanceof DFuncProjectFieldInfo) {
        rexInvokers.addAll(fromFuncProjectFieldInfo((DFuncProjectFieldInfo) projectFieldInfo));
        continue;
      }

      rexInvokers.add(new DRexInputRefInvoker(projectFieldInfo.getFieldName(),
          javaTypeToSqlType(projectFieldInfo.getFieldType())));
    }

    return rexInvokers;
  }

}
