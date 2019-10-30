package org.apache.flink.table.exec;

import static org.apache.flink.types.DSqlTypeUtils.isBigDecimal;
import static org.apache.flink.types.DSqlTypeUtils.isBoolean;
import static org.apache.flink.types.DSqlTypeUtils.isNumeric;
import static org.apache.flink.types.DSqlTypeUtils.isString;
import static org.apache.flink.types.DSqlTypeUtils.numberToNumber;
import static org.apache.flink.types.DSqlTypeUtils.stringToNumber;

import java.math.BigDecimal;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.annotation.Internal;
import org.apache.flink.types.DRecordTuple;

@Internal
public class DRexCastInvoker implements DRexInvoker {

  private final SqlTypeName resultType;

  private final DRexInvoker invoker;

  DRexCastInvoker(SqlTypeName resultType, DRexInvoker invoker) {
    this.resultType = resultType;
    this.invoker = invoker;
  }

  @Override
  public Object invoke(DRecordTuple recordTuple) throws DRexInvokeException {
    SqlTypeName from = invoker.getResultType();

    if (from == resultType) {
      return invoker.invoke(recordTuple);
    }

    // String --> Number
    if (isString(from) && isNumeric(resultType)) {
      return stringToNumber((String) invoker.invoke(recordTuple), resultType);
    }

    // String --> Boolean
    if (isString(from) && isBoolean(resultType)) {
      String value = (String) invoker.invoke(recordTuple);
      return Boolean.valueOf(value);
    }

    // String --> BigDecimal
    if (isString(from) && isBigDecimal(resultType)) {
      String value = (String) invoker.invoke(recordTuple);
      return new BigDecimal(value);
    }

    // Boolean --> Number
    if (isBoolean(from) && isNumeric(resultType)) {
      Boolean value = (Boolean) invoker.invoke(recordTuple);
      return value ? 1 : 0;
    }

    // Boolean -> BigDecimal
    if (isBoolean(from) && isBigDecimal(resultType)) {
      Boolean value = (Boolean) invoker.invoke(recordTuple);
      return value ? BigDecimal.ONE : BigDecimal.ZERO;
    }

    // Number -> Boolean
    if (isNumeric(from) && isBoolean(resultType)) {
      Number value = (Number) invoker.invoke(recordTuple);
      return value.doubleValue() != 0.0d;
    }

    // BigDecimal -> Boolean
    if (isBigDecimal(from) && isBoolean(resultType)) {
      BigDecimal value = (BigDecimal) invoker.invoke(recordTuple);
      return value.compareTo(BigDecimal.ZERO) != 0;
    }

    // Number --> String
    if (isNumeric(from) && isString(resultType)) {
      Object value = invoker.invoke(recordTuple);
      return String.valueOf(value);
    }

    // Boolean --> String
    if (isBoolean(from) && isString(resultType)) {
      Object value = invoker.invoke(recordTuple);
      return String.valueOf(value);
    }

    // BigDecimal --> String
    if (isBigDecimal(from) && isString(resultType)) {
      BigDecimal value = (BigDecimal) invoker.invoke(recordTuple);
      return value.toString();
    }

    // Number, BigDecimal -> Number, BigDecimal
    if ((isNumeric(from) && isNumeric(resultType)) ||
        isNumeric(from) && isBigDecimal(resultType) ||
        isBigDecimal(from) && isNumeric(resultType)) {
      return numberToNumber((Number) invoker.invoke(recordTuple), from, resultType);
    }

    throw new DRexUnsupportedException("Unsupported cast from '"+  from + "' to '" + resultType + "'.");
  }

  @Override
  public SqlTypeName getResultType() {
    return resultType;
  }



}
