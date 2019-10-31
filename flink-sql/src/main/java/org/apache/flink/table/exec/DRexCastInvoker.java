package org.apache.flink.table.exec;

import static org.apache.flink.types.DSqlTypeUtils.booleanToNumber;
import static org.apache.flink.types.DSqlTypeUtils.booleanToString;
import static org.apache.flink.types.DSqlTypeUtils.isBigDecimal;
import static org.apache.flink.types.DSqlTypeUtils.isBoolean;
import static org.apache.flink.types.DSqlTypeUtils.isNumeric;
import static org.apache.flink.types.DSqlTypeUtils.isString;
import static org.apache.flink.types.DSqlTypeUtils.numberToBoolean;
import static org.apache.flink.types.DSqlTypeUtils.numberToNumber;
import static org.apache.flink.types.DSqlTypeUtils.numberToString;
import static org.apache.flink.types.DSqlTypeUtils.stringToBoolean;
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
      return stringToBoolean((String) invoker.invoke(recordTuple));
    }

    // String --> BigDecimal
    if (isString(from) && isBigDecimal(resultType)) {
      return stringToNumber((String) invoker.invoke(recordTuple), resultType);
    }

    // Boolean --> Number
    if (isBoolean(from) && isNumeric(resultType)) {
      return booleanToNumber((boolean) invoker.invoke(recordTuple), resultType);
    }

    // Boolean -> BigDecimal
    if (isBoolean(from) && isBigDecimal(resultType)) {
      return booleanToNumber((boolean) invoker.invoke(recordTuple), resultType);
    }

    // Number -> Boolean
    if (isNumeric(from) && isBoolean(resultType)) {
      return numberToBoolean((Number) invoker.invoke(recordTuple));
    }

    // BigDecimal -> Boolean
    if (isBigDecimal(from) && isBoolean(resultType)) {
      return numberToBoolean((Number) invoker.invoke(recordTuple));
    }

    // Number --> String
    if (isNumeric(from) && isString(resultType)) {
      return numberToString(invoker.invoke(recordTuple));
    }

    // Boolean --> String
    if (isBoolean(from) && isString(resultType)) {
      return booleanToString((boolean) invoker.invoke(recordTuple));
    }

    // BigDecimal --> String
    if (isBigDecimal(from) && isString(resultType)) {
      return numberToString(invoker.invoke(recordTuple));
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
