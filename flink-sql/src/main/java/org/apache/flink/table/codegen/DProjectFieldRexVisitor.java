package org.apache.flink.table.codegen;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.DIVIDE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MINUS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MOD;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MULTIPLY;
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
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.utils.ScalarSqlFunction;
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils;

/**
 *
 * Select name, CAST(age)
 *
 * @author hanhan.zhang
 * **/
public class DProjectFieldRexVisitor implements RexVisitor<DProjectFieldInvoker> {

  private final List<RelDataTypeField> relFieldDataTypes;

  public DProjectFieldRexVisitor(RelDataType relDataType) {
    relFieldDataTypes = relDataType.getFieldList();
  }

  @Override
  public DProjectFieldInvoker visitInputRef(RexInputRef rexInputRef) {
    int index = rexInputRef.getIndex();
    if (index >= relFieldDataTypes.size()) {
      throw new ArrayIndexOutOfBoundsException("Project select field index error, index: " + index);
    }

    RelDataTypeField fieldDataType = relFieldDataTypes.get(index);

    DRexInputRefInvoker expressionInvoker = new DRexInputRefInvoker(fieldDataType.getName(),
        fieldDataType.getType().getSqlTypeName());

    return new DProjectFieldInvoker(expressionInvoker);
  }

  @Override
  public DProjectFieldInvoker visitLocalRef(RexLocalRef rexLocalRef) {
    throw new CodeGenException("Local variables are not supported yet.");
  }

  @Override
  public DProjectFieldInvoker visitLiteral(RexLiteral rexLiteral) {
    // 常量
    // TODO: 类型
    String literalValue = String.valueOf(rexLiteral.getValue3());
    DRexLiteralInvoker literalInvoker = new DRexLiteralInvoker(literalValue, rexLiteral.getType().getSqlTypeName());
    return new DProjectFieldInvoker(literalInvoker);
  }

  @Override
  public DProjectFieldInvoker visitCall(RexCall rexCall) {
    SqlTypeName resultType = rexCall.getType().getSqlTypeName();
    /*
     * 1: 算法表达式
     *
     * 2: 支持UDF解析
     * */
    SqlOperator operator = rexCall.getOperator();
    // 算术
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
  public DProjectFieldInvoker visitOver(RexOver rexOver) {
    return null;
  }

  @Override
  public DProjectFieldInvoker visitCorrelVariable(RexCorrelVariable rexCorrelVariable) {
    return null;
  }

  @Override
  public DProjectFieldInvoker visitDynamicParam(RexDynamicParam rexDynamicParam) {
    return null;
  }

  @Override
  public DProjectFieldInvoker visitRangeRef(RexRangeRef rexRangeRef) {
    return null;
  }

  @Override
  public DProjectFieldInvoker visitFieldAccess(RexFieldAccess rexFieldAccess) {
    return null;
  }

  @Override
  public DProjectFieldInvoker visitSubQuery(RexSubQuery rexSubQuery) {
    return null;
  }

  @Override
  public DProjectFieldInvoker visitTableInputRef(RexTableInputRef rexTableInputRef) {
    return null;
  }

  @Override
  public DProjectFieldInvoker visitPatternFieldRef(RexPatternFieldRef rexPatternFieldRef) {
    return null;
  }

  private static DProjectFieldInvoker generateArithmeticOperator(SqlTypeName resultType, ArithmeticType type,
      List<RexNode> operands, RexVisitor<DProjectFieldInvoker> visitor) {
    assert operands.size() == 2;
    DProjectFieldInvoker left = operands.get(0).accept(visitor);
    DProjectFieldInvoker right = operands.get(1).accept(visitor);
    DFieldArithmeticInvoker fieldArithmeticInvoker = new DFieldArithmeticInvoker(resultType, type, left, right);
    return new DProjectFieldInvoker(fieldArithmeticInvoker);
  }
}
