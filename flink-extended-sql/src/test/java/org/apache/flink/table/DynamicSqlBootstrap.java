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
import org.apache.flink.table.plan.rules.datastream.DynamicDataStreamSinkRule;
import org.apache.flink.table.plan.rules.datastream.DynamicStreamTableSourceScanRule;

public class DynamicSqlBootstrap {

  public static void main(String[] args) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    EnvironmentSettings settings = EnvironmentSettings.newInstance()
        .inStreamingMode()
        .useOldPlanner()
        .build();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
    TableConfig tableConfig = tableEnv.getConfig();

    // 物理执行计划
    RuleSet dynamicStreamRuleSet = RuleSets.ofList(
        DynamicDataStreamScanRule.INSTANCE(),
        DynamicStreamTableSourceScanRule.INSTANCE(),
        DynamicDataStreamCalcRule.INSTANCE(),
        DynamicDataStreamSinkRule.INSTANCE()
    );

    CalciteConfigBuilder configBuilder = CalciteConfig.createBuilder();
    configBuilder.replacePhysicalOptRuleSet(dynamicStreamRuleSet);
    CalciteConfig calciteConfig = configBuilder.build();

    tableConfig.setPlannerConfig(calciteConfig);

    DynamicStreamCatalog catalog = new DynamicStreamCatalog("extended", "stream");
    tableEnv.registerCatalog("extended", catalog);
    tableEnv.useCatalog("extended");
    tableEnv.useDatabase("stream");

    // 输入表
    String sourceTable = "CREATE TABLE t1 (\n"
                       + "   uid bigint, \n"
                       + "   uname varchar comment '姓名', \n"
                       + "   sex varchar comment '性别', \n"
                       + "   age int comment '年龄', \n"
                       + "   action varchar comment '行为', \n"
                       + "   `timestamp` bigint comment '时间戳' \n"
                       + ") WITH ( \n"
                       + "   'connector.type' = 'FAKE' \n"
                       + ")";
    System.out.println(sourceTable);
    tableEnv.executeSql(sourceTable);

    // 输出表
    String sinkTable = "CREATE Table t2 (\n"
                     + "   uname varchar, \n"
                     + "   sex varchar, \n"
                     + "   age int, \n"
                     + "   action varchar, \n"
                     + "   `timestamp` bigint \n"
                     + ") WITH ( \n"
                     + "    'connector.type' = 'CONSOLE' \n"
                     + ")";
    tableEnv.executeSql(sinkTable);
    System.out.println(sinkTable);

    //
    String sql = "INSERT INTO t2 \n"
               + "SELECT \n"
               + "   uname, sex, age, action, `timestamp` \n"
               + "FROM \n"
               + "   t1 \n"
               + "WHERE \n"
               + "   sex = '男'";
    System.out.println(sql);
    System.out.println(tableEnv.explainSql(sql));
    tableEnv.executeSql(sql);



  }

}
