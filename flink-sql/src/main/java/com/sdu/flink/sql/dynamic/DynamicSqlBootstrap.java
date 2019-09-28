package com.sdu.flink.sql.dynamic;

import com.sdu.flink.source.UserActionSourceFunction;
import com.sdu.flink.utils.UserActionEntry;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.DynamicStreamTableSourceImpl;
import org.apache.flink.table.api.DynamicTableEnvironmentUtils;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.SimpleSqlElement;

/**
 * @author hanhan.zhang
 * */
public class DynamicSqlBootstrap {

  public static void main(String[] args) throws Exception {

    String[] actions = new String[] {"Login", "Buy", "Pay", "Exit"};

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // 设置watermark间隔
    env.getConfig().setAutoWatermarkInterval(1000);

    StreamTableEnvironment tableEnv = DynamicTableEnvironmentUtils.create(env);
//    TableConfig config = tableEnv.getConfig();
//    PlannerConfig plannerConfig = CalciteConfig.createBuilder()
//        .replaceLogicalOptRuleSet(DynamicFlinkRuleSets.LOGICAL_OPT_RULES)
//        .build();
//    config.setPlannerConfig(plannerConfig);

    // 数据源
    TableSchema tableSchema = TableSchema.builder()
        .field("uid", DataTypes.STRING())
        .field("action", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .build();
    MapFunction<UserActionEntry, SimpleSqlElement> mapFunction = (UserActionEntry entry) -> {
      Map<String, String> elements = new HashMap<>();
      elements.put("uid", entry.getUid());
      elements.put("action", entry.getAction());
      elements.put("timestamp", String.valueOf(entry.getTimestamp()));
      return SimpleSqlElement.ofElement(elements);
    };

    // Schema
    SourceFunction<SimpleSqlElement> schemaCheckSourceFunction = new SchemaSourceFunction(2000);

    DynamicStreamTableSourceImpl<UserActionEntry> tableSource = new DynamicStreamTableSourceImpl<>(
        new UserActionSourceFunction(actions), mapFunction, tableSchema, schemaCheckSourceFunction, "", 2000);
    tableEnv.registerTableSource("user_action", tableSource);

    // 注册输出
    tableEnv.registerTableSink("user_behavior", new SimplePrintDynamicTableSink(tableSchema));


    String sqlText = "INSERT INTO user_behavior SELECT uid, action FROM user_action where action <> 'Login'";

    tableEnv.sqlUpdate(sqlText);

    tableEnv.execute("DynamicSqlBootstrap");

  }

}
