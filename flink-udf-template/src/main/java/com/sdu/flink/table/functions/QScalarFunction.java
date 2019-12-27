package com.sdu.flink.table.functions;

import com.sdu.flink.table.functions.enhance.DefaultValueCreator;
import com.sdu.flink.table.metric.TimeWindowHistogram;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class QScalarFunction extends ScalarFunction
    implements UserFunctionInterceptor, DefaultValueCreator {

  private static final Logger LOG = LoggerFactory.getLogger(QScalarFunction.class);

  // 指标名
  private static final String EXECUTE_COST = "%s.execute.cost";
  private static final String INVOKE_COUNT = "%s.invoke.count";
  private static final String INVOKE_FAILURE_COUNT = "%s.invoke.failure.count";

  // 发生异常时, 是否直接抛出
  private static final String THROW_EXCEPTION = "throw.exception";

  // 统计耗时指标
  private Histogram executeCostHistogram;
  // 统计调用次数指标
  private Counter invokeCounter;
  // 统计失败次数指标
  private Counter invokeFailureCounter;

  protected boolean throwException;

  private long startTime;

  @Override
  public void open(FunctionContext context) throws Exception {
    throwException = Boolean.parseBoolean(context.getJobParameter(THROW_EXCEPTION, "false"));

    // 注册指标
    executeCostHistogram = context.getMetricGroup().histogram(String.format(EXECUTE_COST, getMetricPrefix()),
        new TimeWindowHistogram());
    invokeCounter = context.getMetricGroup().counter(INVOKE_COUNT);
    invokeFailureCounter = context.getMetricGroup().counter(INVOKE_FAILURE_COUNT);
  }


  @Override
  public void before() {
    startTime = System.currentTimeMillis();
    invokeCounter.inc(1);

  }

  @Override
  public void after() {
    executeCostHistogram.update(System.currentTimeMillis() - startTime);
  }

  @Override
  public void onException(Throwable cause) {
    LOG.error("ScalarFunction({}) occur exception", getMetricPrefix(), cause);
    invokeFailureCounter.inc(1);
  }


  public abstract String getMetricPrefix();
}
