package org.apache.flink.table.exec;

import static org.apache.flink.types.DTypeUtils.isBoolean;
import static org.apache.flink.types.DTypeUtils.isNumeric;
import static org.apache.flink.types.DTypeUtils.isString;

import org.apache.calcite.sql.type.SqlTypeName;
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
    Object leftValue = left.invoke(recordTuple);
    Object rightValue = right.invoke(recordTuple);

    switch (type) {
      case EQUALS:
        return leftValue != null && leftValue.equals(rightValue);
      case NOT_EQUALS:
        return leftValue != null && !leftValue.equals(rightValue);
      case GREATER_THAN:
        return generateGreaterThan(leftValue, left.getResultType(), rightValue, right.getResultType());
      case GREATER_THAN_OR_EQUAL:
        return generateGreaterThanOrEquals(leftValue, left.getResultType(), rightValue, right.getResultType());
      case LESS_THAN:
        return generateLessThan(leftValue, left.getResultType(), rightValue, right.getResultType());
      case LESS_THAN_OR_EQUAL:
        return generateLessThanOrEquals(leftValue, left.getResultType(), rightValue, right.getResultType());
      case IS_NOT_NULL:
        return leftValue != null;
      case IS_NULL:
        return leftValue == null;
      case IS_NOT_DISTINCT_FROM:

      default:
        // TODO: 2019-10-28
        return false;
    }
  }

  @SuppressWarnings("unchecked")
  private static boolean generateGreaterThan(Object leftValue, SqlTypeName leftType,
      Object rightValue, SqlTypeName rightType) {
    /*
     * 数字类型
     * */
    if (isNumeric(leftType) && isNumeric(rightType)) {
      Number lv = (Number) leftValue;
      Number rv = (Number) rightValue;
      return lv.doubleValue() > rv.doubleValue();
    }

    /*
     * 字符串类型
     * */
    if (isString(leftType) && isString(rightType)) {
      String lv = String.valueOf(leftValue);
      String rv = String.valueOf(rightValue);

      return lv.compareTo(rv) > 0;
    }

    /*
     * 布尔类型
     * */
    if (isBoolean(leftType) && isBoolean(rightType)) {
      throw new DRexUnsupportedException("Unsupported boolean comparison: > .");
    }

    /*
     * 实现Comparable接口
     * */
    if (isComparable(leftValue) && isComparable(rightValue)
        && leftValue.getClass() == rightValue.getClass()) {
      Comparable lv = (Comparable) leftValue;
      Comparable rv = (Comparable) rightValue;
      return lv.compareTo(rv) > 0;
    }

    throw new DRexUnsupportedException("Incomparable types: " + leftType + " and " + rightType);
  }

  @SuppressWarnings("unchecked")
  private static boolean generateGreaterThanOrEquals(Object leftValue, SqlTypeName leftType,
      Object rightValue, SqlTypeName rightType) {
    /*
     * 数字类型
     * */
    if (isNumeric(leftType) && isNumeric(rightType)) {
      Number lv = (Number) leftValue;
      Number rv = (Number) rightValue;
      return lv.doubleValue() >= rv.doubleValue();
    }

    /*
     * 字符串类型
     * */
    if (isString(leftType) && isString(rightType)) {
      String lv = String.valueOf(leftValue);
      String rv = String.valueOf(rightValue);

      return lv.compareTo(rv) >= 0;
    }


    /*
     * 布尔类型
     * */
    if (isBoolean(leftType) && isBoolean(rightType)) {
      throw new DRexUnsupportedException("Unsupported boolean comparison: >= .");
    }

    /*
     * 实现Comparable接口
     * */
    if (isComparable(leftValue) && isComparable(rightValue)
        && leftValue.getClass() == rightValue.getClass()) {
      Comparable lv = (Comparable) leftValue;
      Comparable rv = (Comparable) rightValue;
      return lv.compareTo(rv) > 0;
    }

    throw new DRexUnsupportedException("Incomparable types: " + leftType + " and " + rightType);
  }

  @SuppressWarnings("unchecked")
  private static boolean generateLessThan(Object leftValue, SqlTypeName leftType, Object rightValue, SqlTypeName rightType) {
    /*
     * 数字类型
     * */
    if (isNumeric(leftType) && isNumeric(rightType)) {
      Number lv = (Number) leftValue;
      Number rv = (Number) rightValue;
      return lv.doubleValue() < rv.doubleValue();
    }

    /*
     * 字符串类型
     * */
    if (isString(leftType) && isString(rightType)) {
      String lv = String.valueOf(leftValue);
      String rv = String.valueOf(rightValue);

      return lv.compareTo(rv) < 0;
    }

    /*
     * 布尔类型
     * */
    if (isBoolean(leftType) && isBoolean(rightType)) {
      throw new DRexUnsupportedException("Unsupported boolean comparison: < .");
    }

    /*
     * 实现Comparable接口
     * */
    if (isComparable(leftValue) && isComparable(rightValue)
        && leftValue.getClass() == rightValue.getClass()) {
      Comparable lv = (Comparable) leftValue;
      Comparable rv = (Comparable) rightValue;
      return lv.compareTo(rv) > 0;
    }

    throw new DRexUnsupportedException("Incomparable types: " + leftType + " and " + rightType);
  }

  @SuppressWarnings("unchecked")
  private static boolean generateLessThanOrEquals(Object leftValue, SqlTypeName leftType, Object rightValue, SqlTypeName rightType) {
    /*
     * 数字类型
     * */
    if (isNumeric(leftType) && isNumeric(rightType)) {
      Number lv = (Number) leftValue;
      Number rv = (Number) rightValue;
      return lv.doubleValue() <= rv.doubleValue();
    }

    /*
     * 字符串类型
     * */
    if (isString(leftType) && isString(rightType)) {
      String lv = String.valueOf(leftValue);
      String rv = String.valueOf(rightValue);

      return lv.compareTo(rv) <= 0;
    }

    /*
     * 布尔类型
     * */
    if (isBoolean(leftType) && isBoolean(rightType)) {
      throw new DRexUnsupportedException("Unsupported boolean comparison: <= .");
    }

    /*
     * 实现Comparable接口
     * */
    if (isComparable(leftValue) && isComparable(rightValue)
        && leftValue.getClass() == rightValue.getClass()) {
      Comparable lv = (Comparable) leftValue;
      Comparable rv = (Comparable) rightValue;
      return lv.compareTo(rv) > 0;
    }

    throw new DRexUnsupportedException("Incomparable types: " + leftType + " and " + rightType);
  }

  private static boolean isComparable(Object value) {
    return Comparable.class.isAssignableFrom(value.getClass());
  }

  public enum RexCompareType {

    EQUALS, IS_NOT_DISTINCT_FROM, NOT_EQUALS, GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL,

    IS_NULL, IS_NOT_NULL

  }

}
