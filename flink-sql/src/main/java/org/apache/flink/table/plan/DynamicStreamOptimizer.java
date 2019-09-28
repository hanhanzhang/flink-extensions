package org.apache.flink.table.plan;

import static org.apache.flink.table.plan.nodes.DynamicFlinkConventions.DYNAMIC_DATA_STREAM;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.flink.table.calcite.CalciteConfig;
import org.apache.flink.table.calcite.RelTimeIndicatorConverter;
import org.apache.flink.table.plan.rules.DynamicDataStreamCalcRule;
import org.apache.flink.table.plan.rules.DynamicStreamTableSourceScanRule;
import org.apache.flink.table.planner.PlanningConfigurationBuilder;
import scala.Function0;

public class DynamicStreamOptimizer extends StreamOptimizer {

  public DynamicStreamOptimizer(Function0<CalciteConfig> calciteConfig,
      PlanningConfigurationBuilder planningConfigurationBuilder) {
    super(calciteConfig, planningConfigurationBuilder);
  }

  @Override
  public RuleSet getBuiltInPhysicalOptRuleSet() {
    // FlinkRuleSets.DATASTREAM_OPT_RULES
    return RuleSets.ofList(
//        DataStreamSortRule.INSTANCE(),
//        DataStreamGroupAggregateRule.INSTANCE(),
//        DataStreamOverAggregateRule.INSTANCE(),
//        DataStreamGroupWindowAggregateRule.INSTANCE(),
//        DynamicDataStreamCalcRule.INSTANCE,
//        DataStreamScanRule.INSTANCE(),
//        DataStreamUnionRule.INSTANCE(),
//        DataStreamValuesRule.INSTANCE(),
//        DataStreamCorrelateRule.INSTANCE(),
//        DataStreamWindowJoinRule.INSTANCE(),
//        DataStreamJoinRule.INSTANCE(),
//        DataStreamTemporalTableJoinRule.INSTANCE(),
        DynamicStreamTableSourceScanRule.INSTANCE,
        DynamicDataStreamCalcRule.INSTANCE
//        DataStreamMatchRule.INSTANCE(),
//        DataStreamTableAggregateRule.INSTANCE(),
//        DataStreamGroupWindowTableAggregateRule.INSTANCE()
    );
  }

  @Override
  public RelNode optimizePhysicalPlan(RelNode relNode, Convention convention) {
//    return super.optimizePhysicalPlan(relNode, convention);
    return runHepPlannerSequentially(
        HepMatchOrder.TOP_DOWN,
        getBuiltInPhysicalOptRuleSet(),
        relNode,
        relNode.getTraitSet());
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
