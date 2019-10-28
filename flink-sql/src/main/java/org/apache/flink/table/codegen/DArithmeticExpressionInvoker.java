package org.apache.flink.table.codegen;

import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.DOUBLE;
import static org.apache.calcite.sql.type.SqlTypeName.FLOAT;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.SMALLINT;
import static org.apache.calcite.sql.type.SqlTypeName.TINYINT;

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
    if (!isNumeric(left.getResultType())) {
      throw new DRexInvokeException("SqlTypeName should be number type, type: " + left.getResultType());
    }

    if (!isNumeric(right.getResultType())) {
      throw new DRexInvokeException("SqlTypeName should be number type, type: " + right.getResultType());
    }

    Object leftValue = left.invoke(recordTuple);
    Object rightValue = right.invoke(recordTuple);

    switch (arithmeticType) {
      case MINUS:
        if (canCastToDouble(left.getResultType()) || canCastToDouble(right.getResultType())) {
          double lv = ((Number) leftValue).doubleValue();
          double rv = ((Number) rightValue).doubleValue();
          return lv - rv;
        } else {
          int lv = ((Number) leftValue).intValue();
          double rv = ((Number) rightValue).intValue();

          return lv - rv;
        }
      case PLUS:
        if (canCastToDouble(left.getResultType()) || canCastToDouble(right.getResultType())) {
          double lv = ((Number) leftValue).doubleValue();
          double rv = ((Number) rightValue).doubleValue();
          return lv + rv;
        } else {
          int lv = ((Number) leftValue).intValue();
          double rv = ((Number) rightValue).intValue();

          return lv + rv;
        }
      case MOD:
        if (canCastToDouble(left.getResultType()) || canCastToDouble(right.getResultType())) {
          double lv = ((Number) leftValue).doubleValue();
          double rv = ((Number) rightValue).doubleValue();
          return lv % rv;
        } else {
          int lv = ((Number) leftValue).intValue();
          double rv = ((Number) rightValue).intValue();

          return lv % rv;
        }
      case DIVIDE:
      case DIVIDE_INTEGER:
        if (canCastToDouble(left.getResultType()) || canCastToDouble(right.getResultType())) {
          double lv = ((Number) leftValue).doubleValue();
          double rv = ((Number) rightValue).doubleValue();
          return lv / rv;
        } else {
          int lv = ((Number) leftValue).intValue();
          double rv = ((Number) rightValue).intValue();

          return lv / rv;
        }
      case MULTIPLY:
        if (canCastToDouble(left.getResultType()) || canCastToDouble(right.getResultType())) {
          double lv = ((Number) leftValue).doubleValue();
          double rv = ((Number) rightValue).doubleValue();
          return lv * rv;
        } else {
          int lv = ((Number) leftValue).intValue();
          double rv = ((Number) rightValue).intValue();

          return lv * rv;
        }
      default:
        throw new DRexInvokeException("Unsupported arithmetic type: " + arithmeticType);
    }

  }

  @Override
  public SqlTypeName getResultType() {
    return resultType;
  }

  private static boolean isNumeric(SqlTypeName sqlTypeName) {
    return sqlTypeName == INTEGER || sqlTypeName == TINYINT || sqlTypeName == SMALLINT || sqlTypeName == BIGINT
        || sqlTypeName == FLOAT || sqlTypeName == DOUBLE;

  }

  private static boolean canCastToLong(SqlTypeName sqlTypeName) {
    return sqlTypeName == INTEGER || sqlTypeName == TINYINT || sqlTypeName == SMALLINT || sqlTypeName == BIGINT;
  }

  private static boolean canCastToDouble(SqlTypeName sqlTypeName) {
    return sqlTypeName == FLOAT || sqlTypeName == DOUBLE;
  }

  public enum ArithmeticType {
    PLUS, MINUS, MULTIPLY, DIVIDE, DIVIDE_INTEGER, MOD
  }

}
