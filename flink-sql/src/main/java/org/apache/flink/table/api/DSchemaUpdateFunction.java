package org.apache.flink.table.api;

import com.google.gson.reflect.TypeToken;
import com.sdu.flink.utils.JsonUtils;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.plan.nodes.datastream.DDataStreamCalc;
import org.apache.flink.table.updater.DSqlUpdater;
import org.apache.flink.table.updater.DTableStatement;
import org.apache.flink.types.DSchemaTuple;
import org.apache.flink.types.DSchemaTupleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Internal
class DSchemaUpdateFunction implements SourceFunction<DSchemaTuple>, Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(DSchemaUpdateFunction.class);

  private volatile boolean running;

  private final ArrayDeque<DSchemaTuple> schemaQueue;

  DSchemaUpdateFunction() {
    schemaQueue = new ArrayDeque<>(32);
  }

  @Override
  public void run(SourceContext<DSchemaTuple> ctx) throws Exception {
    running = true;
    // 启动SQL监控线程
    Thread sqlMonitorThread = new Thread(this, "sql_update_monitor_thread");
    sqlMonitorThread.start();

    while (running) {
      synchronized (schemaQueue) {
        if (schemaQueue.isEmpty()) {
          schemaQueue.wait();
        }

        if (!running) {
          break;
        }

        DSchemaTuple schemaTuple = schemaQueue.poll();
        if (schemaTuple != null) {
          ctx.collect(schemaTuple);
        }
      }
    }
  }

  @Override
  public void cancel() {
    running = false;
    schemaQueue.notify();
  }

  @Override
  public void run() {
    // 加载配置
    Properties props = new Properties();
    try {
      props.load(getClass().getResourceAsStream("/sql.properties"));
    } catch (IOException e) {
      LOG.error("load config failure", e);
    }

    List<DTableStatement> tableStatements = JsonUtils.fromJson(props.getProperty("table"),
        new TypeToken<List<DTableStatement>>(){}.getType());

    int i = 0;
    int len = props.size() - 1;

    while (running) {
      try {
        TimeUnit.SECONDS.sleep(10);

        DSchemaTuple schemaTuple = new DSchemaTuple();
        int sqlKey = i++ % len;
        String sql = props.getProperty(String.valueOf(sqlKey));
        DDataStreamCalc streamRel = DSqlUpdater.parseSql(tableStatements, sql);

        DSchemaTupleUtils.toSchemaTuple(streamRel, schemaTuple);
        synchronized (schemaQueue) {
          schemaQueue.offer(schemaTuple);
          schemaQueue.notify();
        }
      } catch (Exception e) {
        LOG.error("parse sql failure", e);
      }
    }
  }



}
