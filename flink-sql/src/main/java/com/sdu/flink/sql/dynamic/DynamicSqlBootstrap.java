package com.sdu.flink.sql.dynamic;

import static org.apache.flink.configuration.DSqlOptions.DYNAMIC_SQL_ENABLE;

import com.sdu.flink.sink.SimplePrintAppendSink;
import com.sdu.flink.source.UserActionSourceFunction;
import com.sdu.flink.sql.source.DynamicStreamTableSource;
import com.sdu.flink.utils.UserActionEntry;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.ExecutionConfig.GlobalJobParameters;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author hanhan.zhang
 * */
public class DynamicSqlBootstrap {

  public static void main(String[] args) throws Exception {

    String[] actions = new String[] {"Login", "Buy", "Pay", "Exit"};

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // 设置watermark间隔
    env.getConfig().setAutoWatermarkInterval(1000);
    env.getConfig().setGlobalJobParameters(new GlobalJobParameters() {
      @Override
      public Map<String, String> toMap() {
        Map<String, String> params = new HashMap<>();

        params.put(DYNAMIC_SQL_ENABLE, "true");

        return params;
      }
    });

    EnvironmentSettings settings = EnvironmentSettings.newInstance()
        .inStreamingMode()
        .useOldPlanner()
        .build();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

    // 注册输入
    String[] selectNames = new String[] {"uid", "action"};

    // 全量数据列
    TableSchema sourceTableSchema = new TableSchema.Builder()
        .field("uid", DataTypes.STRING())
        .field("action", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .build();

    // 全量数据
    MapFunction<UserActionEntry, Row> mapFunction = (UserActionEntry entry) -> {
      String uid = entry.getUid();
      String action = entry.getAction();
      long timestamp = entry.getTimestamp();

      Row row = new Row(3);
      row.setField(0, uid);
      row.setField(1, action);
      row.setField(2, timestamp);

      return row;
    };
    DynamicStreamTableSource<UserActionEntry> tableSource = new DynamicStreamTableSource<>(
        new UserActionSourceFunction(actions), mapFunction, sourceTableSchema, selectNames, "", 2000);
    tableEnv.registerTableSource("user_action", tableSource);

    // 注册输出
    TableSchema sinkTableSchema = new TableSchema.Builder()
        .field("uid", DataTypes.STRING())
        .field("action", DataTypes.STRING())
        .build();
    tableEnv.registerTableSink("user_behavior", new SimplePrintAppendSink(sinkTableSchema));


    String sqlText = "INSERT INTO user_behavior SELECT uid, action FROM user_action where action <> 'Login'";

    tableEnv.sqlUpdate(sqlText);

    tableEnv.execute("DynamicSqlBootstrap");

  }

}
