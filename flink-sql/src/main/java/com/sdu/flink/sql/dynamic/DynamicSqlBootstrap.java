package com.sdu.flink.sql.dynamic;

import com.sdu.flink.source.UserActionSourceFunction;
import com.sdu.flink.utils.UserActionEntry;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.DStreamTableSourceImpl;
import org.apache.flink.table.api.DTableEnvironmentUtils;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.DRecordTuple;
import org.apache.flink.types.DSchemaTuple;

/**
 * @author hanhan.zhang
 * */
public class DynamicSqlBootstrap {

  public static void main(String[] args) throws Exception {

    String[] actions = new String[] {"Login", "Buy", "Pay", "Exit"};

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // 设置watermark间隔
    env.getConfig().setAutoWatermarkInterval(1000);

    StreamTableEnvironment tableEnv = DTableEnvironmentUtils.create(env);
//    TableConfig config = tableEnv.getConfig();
//    PlannerConfig plannerConfig = CalciteConfig.createBuilder()
//        .replaceLogicalOptRuleSet(DynamicFlinkRuleSets.LOGICAL_OPT_RULES)
//        .build();
//    config.setPlannerConfig(plannerConfig);

    // TODO: 类型处理
    final Map<String, String> recordTypes = new HashMap<>();
    recordTypes.put("uid", "String");
    recordTypes.put("action", "String");
    recordTypes.put("timestamp", "String");

    // 数据源
    TableSchema tableSchema = TableSchema.builder()
        .field("uid", DataTypes.STRING())
        .field("action", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .build();

    MapFunction<UserActionEntry, DRecordTuple> mapFunction = (UserActionEntry entry) -> {
      Map<String, String> elements = new HashMap<>();
      elements.put("uid", entry.getUid());
      elements.put("action", entry.getAction());
      elements.put("timestamp", String.valueOf(entry.getTimestamp()));
      return new DRecordTuple(recordTypes, elements);
    };

    // Schema
    SourceFunction<DSchemaTuple> schemaCheckSourceFunction = new SchemaSourceFunction(2000);

    DStreamTableSourceImpl<UserActionEntry> tableSource = new DStreamTableSourceImpl<>(
        new UserActionSourceFunction(actions), mapFunction, tableSchema, schemaCheckSourceFunction, "", 2000);
    tableEnv.registerTableSource("user_action", tableSource);

    // 注册UDF
    tableEnv.registerFunction("ADD_PREFIX", new SimpleScalarFunction());

    // 注册输出
    tableEnv.registerTableSink("user_behavior", new SimplePrintDTableSink(tableSchema));


    String sqlText = "INSERT INTO user_behavior SELECT ADD_PREFIX(uid) as userId, action FROM user_action where action <> 'Login' and uid <> '1'";

    tableEnv.sqlUpdate(sqlText);

    tableEnv.execute("DynamicSqlBootstrap");

  }

}
