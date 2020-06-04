package org.apache.flink.table;

import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.calcite.CalciteConfig;
import org.apache.flink.table.calcite.CalciteConfigBuilder;
import org.apache.flink.table.plan.rules.datastream.DynamicDataStreamCalcRule;
import org.apache.flink.table.plan.rules.datastream.DynamicDataStreamScanRule;

public class DynamicSqlBootstrap {

  public static void main(String[] args) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    /*
     * TopN目前仅在BlinkPlaner支持
     * */
    EnvironmentSettings settings = EnvironmentSettings.newInstance()
        .inStreamingMode()
        .useOldPlanner()
        .build();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
    TableConfig tableConfig = tableEnv.getConfig();

    // 物理执行计划
    RuleSet dynamicStreamRuleSet = RuleSets.ofList(
        DynamicDataStreamScanRule.INSTANCE(),
        DynamicDataStreamCalcRule.INSTANCE()
    );

    CalciteConfigBuilder configBuilder = CalciteConfig.createBuilder();
    configBuilder.replacePhysicalOptRuleSet(dynamicStreamRuleSet);
    CalciteConfig calciteConfig = configBuilder.build();

    tableConfig.setPlannerConfig(calciteConfig);


  }

}
