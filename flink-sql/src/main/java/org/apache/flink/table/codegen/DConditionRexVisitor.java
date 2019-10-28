package org.apache.flink.table.codegen;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.AND;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CASE;
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
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT_EQUALS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT_IN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.OR;

import java.util.List;
import java.util.stream.Collectors;
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
import org.apache.flink.table.codegen.DRexCompareInvoker.CompareType;
import org.apache.flink.table.codegen.DRexLogicalInvoker.OperatorType;

public class DConditionRexVisitor implements RexVisitor<DRexFilterInvoker> {

  private final DProjectFieldRexVisitor fieldRexVisitor;

  public DConditionRexVisitor(DProjectFieldRexVisitor fieldRexVisitor) {
    this.fieldRexVisitor = fieldRexVisitor;
  }

  @Override
  public DRexFilterInvoker visitInputRef(RexInputRef rexInputRef) {
    return null;
  }

  @Override
  public DRexFilterInvoker visitLocalRef(RexLocalRef rexLocalRef) {
    return null;
  }

  @Override
  public DRexFilterInvoker visitLiteral(RexLiteral rexLiteral) {
    return null;
  }

  @Override
  public DRexFilterInvoker visitCall(RexCall rexCall) {
    SqlOperator operator = rexCall.getOperator();

    // 比较表达式
    if (operator == EQUALS) {

    }
    else if (operator == IS_NOT_DISTINCT_FROM) {

    }
    else if (operator == NOT_EQUALS) {
      // a != 10
      assert rexCall.getOperands().size() == 2;
      DProjectFieldInvoker leftFieldInvoker = rexCall.getOperands().get(0).accept(fieldRexVisitor);
      DProjectFieldInvoker rightFieldInvoker = rexCall.getOperands().get(1).accept(fieldRexVisitor);
      return new DRexCompareInvoker(leftFieldInvoker, CompareType.NOT_EQUALS, rightFieldInvoker);
    }
    else if (operator == GREATER_THAN) {

    }
    else if (operator == GREATER_THAN_OR_EQUAL) {

    }
    else if (operator == LESS_THAN) {

    }
    else if (operator == LESS_THAN_OR_EQUAL) {

    }
    else if (operator == IS_NULL) {

    }
    else if (operator == IS_NOT_NULL) {

    }

    // 逻辑
    else if (operator == AND) {
      List<DRexFilterInvoker> filterInvokers = rexCall.getOperands()
          .stream()
          .map(rexNode -> rexNode.accept(this))
          .collect(Collectors.toList());
      return new DRexLogicalInvoker(OperatorType.AND, filterInvokers);
    }
    else if (operator == OR) {

    } else if (operator == NOT) {

    } else if (operator == CASE) {

    } else if (operator == IS_TRUE) {

    } else if (operator == IS_NOT_TRUE) {

    } else if (operator == IS_FALSE) {

    } else if (operator == IS_NOT_FALSE) {

    } else if (operator == IN) {

    } else if (operator == NOT_IN) {

    }

    else {
      throw new CodeGenException("Unsupported call: " + rexCall);
    }

    return null;
  }

  @Override
  public DRexFilterInvoker visitOver(RexOver rexOver) {
    return null;
  }

  @Override
  public DRexFilterInvoker visitCorrelVariable(RexCorrelVariable rexCorrelVariable) {
    return null;
  }

  @Override
  public DRexFilterInvoker visitDynamicParam(RexDynamicParam rexDynamicParam) {
    return null;
  }

  @Override
  public DRexFilterInvoker visitRangeRef(RexRangeRef rexRangeRef) {
    return null;
  }

  @Override
  public DRexFilterInvoker visitFieldAccess(RexFieldAccess rexFieldAccess) {
    return null;
  }

  @Override
  public DRexFilterInvoker visitSubQuery(RexSubQuery rexSubQuery) {
    return null;
  }

  @Override
  public DRexFilterInvoker visitTableInputRef(RexTableInputRef rexTableInputRef) {
    return null;
  }

  @Override
  public DRexFilterInvoker visitPatternFieldRef(RexPatternFieldRef rexPatternFieldRef) {
    return null;
  }
}
