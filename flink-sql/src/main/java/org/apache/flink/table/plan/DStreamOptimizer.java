package org.apache.flink.table.plan;

import static org.apache.flink.table.plan.nodes.DFlinkConventions.DYNAMIC_DATA_STREAM;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.flink.table.calcite.CalciteConfig;
import org.apache.flink.table.calcite.RelTimeIndicatorConverter;
import org.apache.flink.table.plan.rules.DDataStreamCalcRule;
import org.apache.flink.table.plan.rules.DStreamTableSourceScanRule;
import org.apache.flink.table.planner.PlanningConfigurationBuilder;
import scala.Function0;

public class DStreamOptimizer extends StreamOptimizer {

  public static RuleSet DATA_STREAM_RULES = RuleSets.ofList(
//        DataStreamSortRule.INSTANCE(),
//        DataStreamGroupAggregateRule.INSTANCE(),
//        DataStreamOverAggregateRule.INSTANCE(),
//        DataStreamGroupWindowAggregateRule.INSTANCE(),
      DDataStreamCalcRule.INSTANCE,
//        DataStreamScanRule.INSTANCE(),
//        DataStreamUnionRule.INSTANCE(),
//        DataStreamValuesRule.INSTANCE(),
//        DataStreamCorrelateRule.INSTANCE(),
//        DataStreamWindowJoinRule.INSTANCE(),
//        DataStreamJoinRule.INSTANCE(),
//        DataStreamTemporalTableJoinRule.INSTANCE(),
      DStreamTableSourceScanRule.INSTANCE
//        DataStreamMatchRule.INSTANCE(),
//        DataStreamTableAggregateRule.INSTANCE(),
//        DataStreamGroupWindowTableAggregateRule.INSTANCE()
  );

  public DStreamOptimizer(Function0<CalciteConfig> calciteConfig,
      PlanningConfigurationBuilder planningConfigurationBuilder) {
    super(calciteConfig, planningConfigurationBuilder);
  }

  @Override
  public RuleSet getBuiltInPhysicalOptRuleSet() {
    return DATA_STREAM_RULES;
  }

  @Override
  public RelNode optimize(RelNode relNode, boolean updatesAsRetraction, RelBuilder relBuilder) {
    RelNode convSubQueryPlan = optimizeConvertSubQueries(relNode);
    RelNode expandedPlan = optimizeExpandPlan(convSubQueryPlan);
    RelNode decorPlan = RelDecorrelator.decorrelateQuery(expandedPlan, relBuilder);
    RelNode planWithMaterializedTimeAttributes =
        RelTimeIndicatorConverter.convert(decorPlan, relBuilder.getRexBuilder());
    RelNode normalizedPlan = optimizeNormalizeLogicalPlan(planWithMaterializedTimeAttributes);
    RelNode logicalPlan = optimizeLogicalPlan(normalizedPlan);

    return optimizePhysicalPlan(logicalPlan, DYNAMIC_DATA_STREAM);

//    return optimizeDecoratePlan(physicalPlan, updatesAsRetraction);
  }

}
