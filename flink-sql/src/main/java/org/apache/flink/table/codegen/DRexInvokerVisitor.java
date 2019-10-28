package org.apache.flink.table.codegen;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.AND;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.DIVIDE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_DISTINCT_FROM;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_NULL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NULL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MINUS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MOD;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MULTIPLY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT_EQUALS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.OR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.PLUS;
import static org.apache.flink.types.DTypeConverts.sqlTypeToJavaType;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.codegen.DFieldArithmeticInvoker.ArithmeticType;
import org.apache.flink.table.codegen.DRexCompareInvoker.RexCompareType;
import org.apache.flink.table.codegen.DRexLogicalInvoker.RexLogicalType;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.utils.ScalarSqlFunction;
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils;

/**
 *
 * @author hanhan.zhang
 * **/
public class DRexInvokerVisitor implements RexVisitor<DRexInvoker<?>> {

  private final List<RelDataTypeField> relFieldDataTypes;

  public DRexInvokerVisitor(RelDataType relDataType) {
    relFieldDataTypes = relDataType.getFieldList();
  }

  @Override
  public DRexInvoker<?> visitInputRef(RexInputRef rexInputRef) {
    /*
     * SQL两种表达式:
     *
     * 1: SELECT field_name
     *
     * 2: WHERE file_name IS NOT NULL
     *
     * NOTE:
     *
     *    翻译为: DRexInputRefInvoker
     * */

    int index = rexInputRef.getIndex();
    if (index >= relFieldDataTypes.size()) {
      throw new ArrayIndexOutOfBoundsException("Project select field index error, index: " + index);
    }

    RelDataTypeField fieldDataType = relFieldDataTypes.get(index);

    return new DRexInputRefInvoker(fieldDataType.getName(), fieldDataType.getType().getSqlTypeName());

  }

  @Override
  public DRexInvoker<?> visitLocalRef(RexLocalRef rexLocalRef) {
    throw new DRexUnsupportedException("Local variables are not supported yet.");
  }

  @Override
  public DRexInvoker<?> visitLiteral(RexLiteral rexLiteral) {
    /*
     * 常量解析:
     *
     * WHERE field_name != 5, 其中5为常量
     *
     * NOTE:
     *
     *  翻译为: DRexLiteralInvoker
     * */
    Object literalValue = rexLiteral.getValue3();
    return new DRexLiteralInvoker(literalValue, rexLiteral.getType().getSqlTypeName());
  }

  @Override
  public DRexInvoker<?> visitCall(RexCall rexCall) {
    /*
     * 解析SQL表达式
     *
     * 1: SELECT UDF(field_name)
     *
     * 2: WHERE field_name != 5 AND field_id IS NOT NULL
     *
     * */
    SqlTypeName resultType = rexCall.getType().getSqlTypeName();

    SqlOperator operator = rexCall.getOperator();
    /*
     * 算数表达式
     * */
    if (operator == PLUS ) {
      List<RexNode> operands = rexCall.getOperands();
      return generateArithmeticOperator(resultType, ArithmeticType.PLUS, operands, this);
    }
    else if (operator == MINUS) {
      List<RexNode> operands = rexCall.getOperands();
      return generateArithmeticOperator(resultType, ArithmeticType.MINUS, operands, this);
    }
    else if (operator == MULTIPLY) {
      List<RexNode> operands = rexCall.getOperands();
      return generateArithmeticOperator(resultType, ArithmeticType.MULTIPLY, operands, this);
    }
    else if (operator == DIVIDE) {
      List<RexNode> operands = rexCall.getOperands();
      return generateArithmeticOperator(resultType, ArithmeticType.DIVIDE, operands, this);
    }
    else if (operator == MOD) {
      List<RexNode> operands = rexCall.getOperands();
      return generateArithmeticOperator(resultType, ArithmeticType.MOD, operands, this);
    }

    /*
     * 逻辑表达式, WHERE条件过滤
     * **/
    if (operator == EQUALS) {
      return generateCompareExpression(rexCall.getOperands(), RexCompareType.EQUALS, this);
    }
    else if (operator == IS_NOT_DISTINCT_FROM) {
      return generateCompareExpression(rexCall.getOperands(), RexCompareType.IS_NOT_DISTINCT_FROM, this);
    }
    else if (operator == NOT_EQUALS) {
      return generateCompareExpression(rexCall.getOperands(), RexCompareType.NOT_EQUALS, this);
    }
    else if (operator == GREATER_THAN) {
      return generateCompareExpression(rexCall.getOperands(), RexCompareType.GREATER_THAN, this);
    }
    else if (operator == GREATER_THAN_OR_EQUAL) {
      return generateCompareExpression(rexCall.getOperands(), RexCompareType.GREATER_THAN_OR_EQUAL, this);
    }
    else if (operator == LESS_THAN) {
      return generateCompareExpression(rexCall.getOperands(), RexCompareType.LESS_THAN, this);
    }
    else if (operator == LESS_THAN_OR_EQUAL) {
      return generateCompareExpression(rexCall.getOperands(), RexCompareType.LESS_THAN_OR_EQUAL, this);
    }
    else if (operator == IS_NULL) {
      RexNode operand = rexCall.getOperands().get(0);
      return generateCompareExpression(operand, RexCompareType.IS_NULL, this);
    }
    else if (operator == IS_NOT_NULL) {
      RexNode operand = rexCall.getOperands().get(0);
      return generateCompareExpression(operand, RexCompareType.IS_NOT_NULL, this);
    }

    /*
     * 逻辑表达式, WHERE条件
     *
     * **/
    else if (operator == AND) {
      return generateLogicalExpression(rexCall.getOperands(), RexLogicalType.AND, this);
    }
    else if (operator == OR) {
      return generateLogicalExpression(rexCall.getOperands(), RexLogicalType.OR, this);
    }
//    else if (operator == NOT) {
//
//    }
//    else if (operator == CASE) {
//
//    }
//    else if (operator == IS_TRUE) {
//
//    }
//    else if (operator == IS_NOT_TRUE) {
//
//    }
//    else if (operator == IS_FALSE) {
//
//    }
//    else if (operator == IS_NOT_FALSE) {
//
//    }
//    else if (operator == IN) {
//
//    }
//    else if (operator == NOT_IN) {
//
//    }

    else {
      // 自定义函数
      final List<DRexInvoker<?>> parameterRexInvokes = new ArrayList<>();
      final List<Class<?>> parameterClassTypes = new ArrayList<>();
      for (RexNode rexNode : rexCall.getOperands()) {
        DRexInvoker<?> expressionInvoker = rexNode.accept(this);
        parameterRexInvokes.add(expressionInvoker);
        parameterClassTypes.add(sqlTypeToJavaType(expressionInvoker.getResultType()));
      }

      if (operator instanceof SqlFunction) {
        SqlFunction function = (SqlFunction) operator;

        if (function instanceof ScalarSqlFunction) {
          ScalarSqlFunction ssf = (ScalarSqlFunction) function;
          ScalarFunction scalarFunction = ssf.getScalarFunction();
          Method[] methods = UserDefinedFunctionUtils.checkAndExtractMethods(scalarFunction, "eval");
          // 根据参数个数, 参数类型, 选取Method
          List<Method> userDefineMethods = Arrays.stream(methods)
              .filter(method -> {
                Class<?>[] parameterTypes = method.getParameterTypes();
                if (parameterTypes.length != parameterClassTypes.size()) {
                  return false;
                }
                for (int i = 0; i < parameterTypes.length; ++i) {
                  if (parameterTypes[i] != parameterClassTypes.get(i)) {
                    return false;
                  }
                }
                return true;
              }).collect(Collectors.toList());

          if (userDefineMethods.isEmpty()) {
            throw new UserDefineFunctionNotFoundException(parameterClassTypes);
          }

          String className = ssf.getScalarFunction().getClass().getCanonicalName();
          DSqlFunctionInvoker expressionInvoker = new DSqlFunctionInvoker(className, parameterRexInvokes, resultType);

          return new DProjectFieldInvoker(expressionInvoker);
        }
      }
    }

    throw new RuntimeException("Unsupported RexCall: " + rexCall.getOperator().getKind());
  }

  @Override
  public DRexInvoker<?> visitOver(RexOver rexOver) {
    throw new DRexUnsupportedException("Aggregate functions over windows are not supported yet.");
  }

  @Override
  public DRexInvoker<?> visitCorrelVariable(RexCorrelVariable rexCorrelVariable) {
    // TODO: 2019-10-28
    throw new DRexUnsupportedException("Correl  variable are not supported yet.");
  }

  @Override
  public DRexInvoker<?> visitDynamicParam(RexDynamicParam rexDynamicParam) {
    throw new DRexUnsupportedException("Dynamic parameter references are not supported yet.");
  }

  @Override
  public DRexInvoker<?> visitRangeRef(RexRangeRef rexRangeRef) {
    throw new DRexUnsupportedException("Range references are not supported yet.");
  }

  @Override
  public DRexInvoker<?> visitFieldAccess(RexFieldAccess rexFieldAccess) {
    // TODO: 2019-10-28
    throw new DRexUnsupportedException("Field access are not supported yet.");
  }

  @Override
  public DRexInvoker<?> visitSubQuery(RexSubQuery rexSubQuery) {
    throw new DRexUnsupportedException("SubQuery are not supported yet.");
  }

  @Override
  public DRexInvoker<?> visitTableInputRef(RexTableInputRef rexTableInputRef) {
    // TODO: 2019-10-28
    throw new DRexUnsupportedException("Table input references are not supported yet.");
  }

  @Override
  public DRexInvoker<?> visitPatternFieldRef(RexPatternFieldRef rexPatternFieldRef) {
    throw new DRexUnsupportedException("Pattern field references are not supported yet.");
  }

  private static DRexInvoker<?> generateArithmeticOperator(SqlTypeName resultType, ArithmeticType type,
      List<RexNode> operands, RexVisitor<DRexInvoker<?>> visitor) {
    assert operands.size() == 2;
    DRexInvoker<?> left = operands.get(0).accept(visitor);
    DRexInvoker<?> right = operands.get(1).accept(visitor);
    return new DFieldArithmeticInvoker(resultType, type, left, right);
  }

  private static DRexInvoker<Boolean> generateCompareExpression(List<RexNode> operands, RexCompareType type, RexVisitor<DRexInvoker<?>> visitor) {
    assert operands.size() == 2;
    DRexInvoker<?> left = operands.get(0).accept(visitor);
    DRexInvoker<?> right = operands.get(1).accept(visitor);
    return new DRexCompareInvoker(left, type, right);
  }

  private static DRexInvoker<Boolean> generateCompareExpression(RexNode operand, RexCompareType type, RexVisitor<DRexInvoker<?>> visitor) {
    DRexInvoker<?> left = operand.accept(visitor);
    return new DRexCompareInvoker(left, type, null);
  }

  private static DRexInvoker<Boolean> generateLogicalExpression(List<RexNode> operands, RexLogicalType type, RexVisitor<DRexInvoker<?>> visitor) {
    assert operands.size() == 2;

    List<DRexInvoker<?>> filterInvokers = new ArrayList<>();
    for (RexNode operand : operands) {
      filterInvokers.add(operand.accept(visitor));
    }
    return new DRexLogicalInvoker(type, filterInvokers);
  }

}
