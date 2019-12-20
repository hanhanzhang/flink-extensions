package org.apache.flink.table.exec;

import static org.apache.calcite.sql.type.SqlTypeName.DOUBLE;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.flink.types.DSqlTypeUtils.canCastToDouble;
import static org.apache.flink.types.DSqlTypeUtils.isBigDecimal;
import static org.apache.flink.types.DSqlTypeUtils.isNumeric;
import static org.apache.flink.types.DSqlTypeUtils.numberToNumber;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.types.DRecordTuple;

public class DArithmeticExpressionInvoker implements DRexInvoker {

  private final SqlTypeName resultType;
  private final ArithmeticType arithmeticType;
  private final DRexInvoker left;
  private final DRexInvoker right;

  DArithmeticExpressionInvoker(SqlTypeName resultType, ArithmeticType arithmeticType, DRexInvoker left, DRexInvoker right) {
    this.resultType = resultType;
    this.arithmeticType = arithmeticType;
    this.left = left;
    this.right = right;
  }

  @Override
  public Object invoke(DRecordTuple recordTuple) throws DRexInvokeException {
    if (!isNumeric(left.getResultType()) && !isBigDecimal(left.getResultType())) {
      throw new DRexInvokeException("SqlTypeName should be number type, type: " + left.getResultType());
    }

    if (!isNumeric(right.getResultType()) && !isBigDecimal(right.getResultType())) {
      throw new DRexInvokeException("SqlTypeName should be number type, type: " + right.getResultType());
    }

    Object leftValue = left.invoke(recordTuple);
    Object rightValue = right.invoke(recordTuple);

    switch (arithmeticType) {
      case MINUS:
        if (canCastToDouble(left.getResultType()) || canCastToDouble(right.getResultType())) {
          double lv = ((Number) leftValue).doubleValue();
          double rv = ((Number) rightValue).doubleValue();
          return numberToNumber(lv - rv, DOUBLE, resultType);
        } else {
          long lv = ((Number) leftValue).longValue();
          long rv = ((Number) rightValue).longValue();
          return numberToNumber(lv - rv, INTEGER, resultType);
        }

      case PLUS:
        if (canCastToDouble(left.getResultType()) || canCastToDouble(right.getResultType())) {
          double lv = ((Number) leftValue).doubleValue();
          double rv = ((Number) rightValue).doubleValue();
          return numberToNumber(lv + rv, DOUBLE, resultType);
        } else {
          long lv = ((Number) leftValue).longValue();
          long rv = ((Number) rightValue).longValue();
          return numberToNumber(lv + rv, INTEGER, resultType);
        }

      case MOD:
        if (canCastToDouble(left.getResultType()) || canCastToDouble(right.getResultType())) {
          double lv = ((Number) leftValue).doubleValue();
          double rv = ((Number) rightValue).doubleValue();
          return numberToNumber(lv % rv, DOUBLE, resultType);
        } else {
          long lv = ((Number) leftValue).longValue();
          long rv = ((Number) rightValue).longValue();
          return numberToNumber(lv % rv, INTEGER, resultType);
        }

      case DIVIDE:
      case DIVIDE_INTEGER:
        if (canCastToDouble(left.getResultType()) || canCastToDouble(right.getResultType())) {
          double lv = ((Number) leftValue).doubleValue();
          double rv = ((Number) rightValue).doubleValue();
          return numberToNumber(lv / rv, DOUBLE, resultType);
        } else {
          long lv = ((Number) leftValue).longValue();
          long rv = ((Number) rightValue).longValue();
          return numberToNumber(lv / rv, INTEGER, resultType);
        }

      case MULTIPLY:
        if (canCastToDouble(left.getResultType()) || canCastToDouble(right.getResultType())) {
          double lv = ((Number) leftValue).doubleValue();
          double rv = ((Number) rightValue).doubleValue();
          return numberToNumber(lv * rv, DOUBLE, resultType);
        } else {
          long lv = ((Number) leftValue).longValue();
          long rv = ((Number) rightValue).longValue();
          return numberToNumber(lv * rv, INTEGER, resultType);
        }

      default:
        throw new DRexInvokeException("Unsupported arithmetic type: " + arithmeticType);
    }

  }

  @Override
  public SqlTypeName getResultType() {
    return resultType;
  }


  public enum ArithmeticType {
    PLUS, MINUS, MULTIPLY, DIVIDE, DIVIDE_INTEGER, MOD
  }

}
