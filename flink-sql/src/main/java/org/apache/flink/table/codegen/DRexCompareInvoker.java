package org.apache.flink.table.codegen;

import org.apache.flink.annotation.Internal;
import org.apache.flink.types.DRecordTuple;
import org.apache.flink.util.Preconditions;

/**
 * 比较表达式:
 *
 * EQUALS, IS_NOT_DISTINCT_FROM, NOT_EQUALS, GREATER_THAN, GREATER_THAN_OR_EQUAL,
 *
 * LESS_THAN, LESS_THAN_OR_EQUAL, IS_NULL, IS_NOT_NULL
 *
 *
 * @author hanhan.zhang
 * */
@Internal
public class DRexCompareInvoker implements DRexFilterInvoker{

  private final DProjectFieldInvoker leftFieldInvoker;

  private final CompareType type;

  private final DProjectFieldInvoker rightFileInvoker;

  public DRexCompareInvoker(DProjectFieldInvoker leftFieldInvoker, CompareType type, DProjectFieldInvoker rightFileInvoker) {
    Preconditions.checkNotNull(leftFieldInvoker);
    Preconditions.checkNotNull(rightFileInvoker);

    // 同种数据类型比较
    Preconditions.checkState(leftFieldInvoker.getResultType() == rightFileInvoker.getResultType());

    this.leftFieldInvoker = leftFieldInvoker;
    this.type = type;
    this.rightFileInvoker = rightFileInvoker;
  }

  @Override
  public Boolean invoke(DRecordTuple recordTuple) throws DExpressionInvokeException {
    String leftField = leftFieldInvoker.invoke(recordTuple);
    String rightField = rightFileInvoker.invoke(recordTuple);

    switch (type) {
      case EQUALS:
        return leftField != null && leftField.equals(rightField);
      case NOT_EQUALS:
        return leftField != null && !leftField.equals(rightField);
      default:
        // TODO: 2019-10-28
        return false;
    }
  }

  public enum CompareType {

    EQUALS, IS_NOT_DISTINCT_FROM, NOT_EQUALS

  }

}
