package org.apache.flink.table.api;

import static java.lang.String.format;

import org.apache.flink.table.util.JsonUtils;
import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.types.SqlSchemaTuple;
import org.apache.flink.table.types.schema.SqlCalcSchema;
import org.apache.flink.table.types.schema.SqlScanSchema;

@VisibleForTesting
public class DynamicSqlMonitorFunction extends RichParallelSourceFunction<SqlSchemaTuple> {

  // TODO: 设计如何给用户开放, 这里仅为测试

  private transient boolean running;
  private transient int nums;

  @Override
  public void open(Configuration parameters) throws Exception {
    this.running = true;
    this.nums = 0;
  }

  @Override
  public void run(SourceContext<SqlSchemaTuple> ctx) throws Exception {
    safeSleep();
    while (running) {
      int i = (nums++ % 4);
//      if (i != 3) {
//        continue;
//      }
      SqlSchemaTuple schemaTuple = create(i);
      ctx.collect(schemaTuple);
      safeSleep();
    }
  }

  @Override
  public void cancel() {
    this.running = false;
  }

  private static void safeSleep() {
    try {
      TimeUnit.SECONDS.sleep(10);
    } catch (Exception e) {
      // ignore
    }
  }

  private static SqlSchemaTuple create(int i) {
    SqlScanSchema scanSchema = new SqlScanSchema(
        Lists.newArrayList("uid", "uname", "sex", "age", "action", "timestamp"),
        getSelectFields(i)
    );

    SqlCalcSchema calcSchema = new SqlCalcSchema(getCodeName(i), getCode(i));

    Map<String, String> map = new HashMap<>();
    map.put("DynamicStreamTableSourceScan", JsonUtils.toJson(scanSchema));
    map.put("DynamicDataStreamCalc#DynamicStreamTableSourceScan", JsonUtils.toJson(calcSchema));

    return new SqlSchemaTuple(map);
  }

  private static List<String> getSelectFields(int i) {
    switch (i) {
      case 0:
      case 1:
        return Lists.newArrayList("uname", "age", "action", "timestamp", "sex");

      case 2:
      case 3:
        return Lists.newArrayList("uname", "action", "timestamp", "sex", "age");

      default:
        throw new RuntimeException("");
    }
  }

  private static String getCode(int i) {
    File codeFile = new File(DynamicSqlMonitorFunction.class.getResource(format("/code%d.txt", i)).getPath());
    byte[] content = new byte[(int) codeFile.length()];
    try {
      FileInputStream in = new FileInputStream(codeFile);
      in.read(content);
      in.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return new String(content);
  }

  private static String getCodeName(int i) {
    switch (i) {
      case 0:
        return "DynamicDataStreamCalcRule$36";

      case 1:
        return "DynamicDataStreamCalcRule$44";

      case 2:
        return "DynamicDataStreamCalcRule$58";

      case 3:
        return "DynamicDataStreamCalcRule$69";

      default:
        throw new RuntimeException("");
    }
  }

}
