package org.apache.flink.table.codegen;

import org.apache.flink.annotation.Internal;
import org.apache.flink.types.DRecordTuple;

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
public class DRexCompareInvoker implements DRexFilterInvoker {

  private final DRexInvoker left;

  private final RexCompareType type;

  private final DRexInvoker right;

  DRexCompareInvoker(DRexInvoker left, RexCompareType type, DRexInvoker right) {
    this.left = left;
    this.type = type;
    this.right = right;
  }

  @Override
  public Boolean invoke(DRecordTuple recordTuple) throws DRexInvokeException {
    // TODO: 2019-10-28 类型处理
    Object leftField = left.invoke(recordTuple);
    Object rightField = right.invoke(recordTuple);

    switch (type) {
      case EQUALS:
        return leftField != null && leftField.equals(rightField);
      case NOT_EQUALS:
        return leftField != null && !leftField.equals(rightField);
     /* case GREATER_THAN:
        return leftField != null && leftField.compareTo(rightField) > 0;
      case GREATER_THAN_OR_EQUAL:
        return leftField != null && leftField.compareTo(rightField) >= 0;
      case LESS_THAN:
        return leftField != null && leftField.compareTo(rightField) < 0;
      case LESS_THAN_OR_EQUAL:
        return leftField != null && leftField.compareTo(rightField) <= 0;*/
      case IS_NOT_NULL:
        return leftField != null;
      case IS_NULL:
        return leftField == null;
      case IS_NOT_DISTINCT_FROM:

      default:
        // TODO: 2019-10-28
        return false;
    }
  }


  public enum RexCompareType {

    EQUALS, IS_NOT_DISTINCT_FROM, NOT_EQUALS, GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL,

    IS_NULL, IS_NOT_NULL


  }

}
