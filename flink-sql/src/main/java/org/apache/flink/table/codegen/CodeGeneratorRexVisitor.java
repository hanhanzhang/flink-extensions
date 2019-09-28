package org.apache.flink.table.codegen;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.AND;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.AS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CARDINALITY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CASE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CAST;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CONCAT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.DIVIDE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.DIVIDE_INTEGER;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.DOT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_FALSE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_DISTINCT_FROM;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_FALSE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_NULL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_TRUE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NULL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_TRUE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ITEM;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MINUS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MINUS_DATE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MOD;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MULTIPLY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT_EQUALS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT_IN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.OR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.PLUS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.REINTERPRET;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ROW;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.UNARY_MINUS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.UNARY_PLUS;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.SqlOperator;
import org.apache.flink.table.runtime.Exepression;
import org.apache.flink.table.runtime.RexType;

public class CodeGeneratorRexVisitor implements RexVisitor<Exepression> {

  private Map<Integer, String> indexToName;

  public CodeGeneratorRexVisitor(RelDataType dataType) {
    List<String> fieldNames = dataType.getFieldNames();
    indexToName = new HashMap<>(fieldNames.size());
    int index = 0;
    for (String name : fieldNames) {
      indexToName.put(index++, name);
    }
  }

  @Override
  public Exepression visitInputRef(RexInputRef rexInputRef) {
    int index = rexInputRef.getIndex();
    if (index >= indexToName.size()) {
      throw new ArrayIndexOutOfBoundsException("Project select field index error, index: " + index);
    }

    return new Exepression(indexToName.get(index), RexType.NAME);
  }

  @Override
  public Exepression visitLocalRef(RexLocalRef rexLocalRef) {
    throw new CodeGenException("Local variables are not supported yet.");
  }

  @Override
  public Exepression visitLiteral(RexLiteral rexLiteral) {
    return null;
  }

  @Override
  public Exepression visitCall(RexCall rexCall) {
    SqlOperator operator = rexCall.getOperator();

    // 算数表达式
    if (operator == PLUS) {

    } else if (operator == MINUS || operator == MINUS_DATE)  {

    } else if (operator == MULTIPLY) {

    } else if (operator == DIVIDE || operator == DIVIDE_INTEGER) {

    } else if (operator == MOD) {

    } else if (operator == UNARY_MINUS) {

    } else if (operator == UNARY_PLUS) {

    }

    // 比较表达式
    else if (operator == EQUALS) {

    } else if (operator == IS_NOT_DISTINCT_FROM) {

    } else if (operator == NOT_EQUALS) {

    } else if (operator == GREATER_THAN) {

    } else if (operator == GREATER_THAN_OR_EQUAL) {

    } else if (operator == LESS_THAN) {

    } else if (operator == LESS_THAN_OR_EQUAL) {

    } else if (operator == IS_NULL) {

    } else if (operator == IS_NOT_NULL) {

    }

    // 逻辑
    else if (operator == AND) {

    } else if (operator == OR) {

    } else if (operator == NOT) {

    } else if (operator == CASE) {

    } else if (operator == IS_TRUE) {

    } else if (operator == IS_NOT_TRUE) {

    } else if (operator == IS_FALSE) {

    } else if (operator == IS_NOT_FALSE) {

    } else if (operator == IN) {

    } else if (operator == NOT_IN) {

    }

    // 转换
    else if (operator == CAST || operator == REINTERPRET) {

    } else if (operator == AS) {

    } else if (operator == CONCAT) {

    }

    else if (operator == ROW) {

    } else if (operator == ARRAY_VALUE_CONSTRUCTOR) {

    } else if (operator == MAP_VALUE_CONSTRUCTOR) {

    } else if (operator == ITEM) {

    } else if (operator == CARDINALITY) {

    } else if (operator == DOT) {

    }

    else {
      throw new CodeGenException("Unsupported call: " + rexCall);
    }

    return null;
  }

  @Override
  public Exepression visitOver(RexOver rexOver) {
    return null;
  }

  @Override
  public Exepression visitCorrelVariable(RexCorrelVariable rexCorrelVariable) {
    return null;
  }

  @Override
  public Exepression visitDynamicParam(RexDynamicParam rexDynamicParam) {
    return null;
  }

  @Override
  public Exepression visitRangeRef(RexRangeRef rexRangeRef) {
    return null;
  }

  @Override
  public Exepression visitFieldAccess(RexFieldAccess rexFieldAccess) {
    return null;
  }

  @Override
  public Exepression visitSubQuery(RexSubQuery rexSubQuery) {
    return null;
  }

  @Override
  public Exepression visitTableInputRef(RexTableInputRef rexTableInputRef) {
    return null;
  }

  @Override
  public Exepression visitPatternFieldRef(RexPatternFieldRef rexPatternFieldRef) {
    return null;
  }
}
