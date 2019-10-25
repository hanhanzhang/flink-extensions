package org.apache.flink.table.codegen;

public interface DProjectFieldExpressionInvoker extends DExpressionInvoker<String> {

  String getProjectFieldName();

}
