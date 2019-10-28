package org.apache.flink.table.codegen;

import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.DOUBLE;
import static org.apache.calcite.sql.type.SqlTypeName.FLOAT;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.SMALLINT;
import static org.apache.calcite.sql.type.SqlTypeName.TINYINT;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.types.DRecordTuple;

public class DFieldArithmeticInvoker implements DRexInvoker<String> {

  private final SqlTypeName resultType;
  private final ArithmeticType arithmeticType;
  private final DProjectFieldInvoker leftFieldInvoker;
  private final DProjectFieldInvoker rightFieldInvoker;

  public DFieldArithmeticInvoker(SqlTypeName resultType, ArithmeticType arithmeticType,
      DProjectFieldInvoker leftFieldInvoker, DProjectFieldInvoker rightFieldInvoker) {
    this.resultType = resultType;
    this.arithmeticType = arithmeticType;
    this.leftFieldInvoker = leftFieldInvoker;
    this.rightFieldInvoker = rightFieldInvoker;
  }

  @Override
  public String invoke(DRecordTuple recordTuple) throws DExpressionInvokeException {
    if (!isNumeric(leftFieldInvoker.getResultType())) {
      throw new DExpressionInvokeException("SqlTypeName should be number type, type: " + leftFieldInvoker.getResultType());
    }

    if (!isNumeric(rightFieldInvoker.getResultType())) {
      throw new DExpressionInvokeException("SqlTypeName should be number type, type: " + rightFieldInvoker.getResultType());
    }

    String leftFieldValue = leftFieldInvoker.invoke(recordTuple);
    String rightFieldValue = rightFieldInvoker.invoke(recordTuple);

    switch (arithmeticType) {
      case MINUS:
        if (canCastToDouble(leftFieldInvoker.getResultType()) || canCastToDouble(rightFieldInvoker.getResultType())) {
          double leftValue = Double.parseDouble(leftFieldValue);
          double rightValue = Double.parseDouble(rightFieldValue);

          return String.valueOf(leftValue - rightValue);
        } else {
          int leftValue = Integer.parseInt(leftFieldValue);
          int rightValue = Integer.parseInt(rightFieldValue);

          return String.valueOf(leftValue - rightValue);
        }
      case PLUS:
        if (canCastToDouble(leftFieldInvoker.getResultType()) || canCastToDouble(rightFieldInvoker.getResultType())) {
          double leftValue = Double.parseDouble(leftFieldValue);
          double rightValue = Double.parseDouble(rightFieldValue);

          return String.valueOf(leftValue + rightValue);
        } else {
          int leftValue = Integer.parseInt(leftFieldValue);
          int rightValue = Integer.parseInt(rightFieldValue);

          return String.valueOf(leftValue + rightValue);
        }
      case MOD:
        if (canCastToDouble(leftFieldInvoker.getResultType()) || canCastToDouble(rightFieldInvoker.getResultType())) {
          double leftValue = Double.parseDouble(leftFieldValue);
          double rightValue = Double.parseDouble(rightFieldValue);

          return String.valueOf(leftValue % rightValue);
        } else {
          int leftValue = Integer.parseInt(leftFieldValue);
          int rightValue = Integer.parseInt(rightFieldValue);

          return String.valueOf(leftValue % rightValue);
        }
      case DIVIDE:
      case DIVIDE_INTEGER:
        if (canCastToDouble(leftFieldInvoker.getResultType()) || canCastToDouble(rightFieldInvoker.getResultType())) {
          double leftValue = Double.parseDouble(leftFieldValue);
          double rightValue = Double.parseDouble(rightFieldValue);

          return String.valueOf(leftValue / rightValue);
        } else {
          int leftValue = Integer.parseInt(leftFieldValue);
          int rightValue = Integer.parseInt(rightFieldValue);

          return String.valueOf(leftValue / rightValue);
        }
      case MULTIPLY:
        if (canCastToDouble(leftFieldInvoker.getResultType()) || canCastToDouble(rightFieldInvoker.getResultType())) {
          double leftValue = Double.parseDouble(leftFieldValue);
          double rightValue = Double.parseDouble(rightFieldValue);

          return String.valueOf(leftValue * rightValue);
        } else {
          int leftValue = Integer.parseInt(leftFieldValue);
          int rightValue = Integer.parseInt(rightFieldValue);

          return String.valueOf(leftValue * rightValue);
        }
      default:
        throw new DExpressionInvokeException("Unsupported arithmetic type: " + arithmeticType);
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
