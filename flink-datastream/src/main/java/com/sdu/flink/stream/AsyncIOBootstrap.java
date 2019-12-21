package com.sdu.flink.stream;

import com.sdu.flink.stream.utils.DataNotFoundException;
import com.sdu.flink.stream.utils.RedisUtils;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.ExecutionConfig.GlobalJobParameters;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.async.queue.StreamRecordQueueEntry;
import org.apache.flink.types.Row;
import redis.clients.jedis.Jedis;

/**
 * @author hanhan.zhang
 */
public class AsyncIOBootstrap {

  private static final String REDIS_HOST = "redis.host";
  private static final String REDIS_PORT = "redis.port";

  private static final List<Row> STUDENTS;
  private static final Map<String, String> PARENTS;

  static {
    STUDENTS = new LinkedList<>();
    STUDENTS.add(Row.of("1", "张小龙", "男", 21));
    STUDENTS.add(Row.of("2", "李小虎", "男", 22));
    STUDENTS.add(Row.of("3", "王彩云", "女", 20));
    STUDENTS.add(Row.of("4", "时晓华", "女", 21));
    STUDENTS.add(Row.of("5", "孙倩雯", "女", 22));
    STUDENTS.add(Row.of("6", "刘翔飞", "男", 19));
    STUDENTS.add(Row.of("7", "陈永康", "男", 21));

    PARENTS = new HashMap<>();
    PARENTS.put("1", "张大海,王小花");
    PARENTS.put("2", "李天霸,郝文青");
    PARENTS.put("3", "王大华,吕小梅");
    PARENTS.put("4", "时大新,李晓颖");
    PARENTS.put("5", "孙鹏飞,孟笑笑");
    PARENTS.put("6", "刘卫东,孙晓慧");
    PARENTS.put("8", "陈飞鹏,鲁天晴");
  }

  public static class AsyncParentDataGetter extends RichAsyncFunction<Row, Row> {

    // Task单线程, 独占一个Redis客户端
    private transient Jedis jedis;

    @Override
    public void open(Configuration parameters) throws Exception {
      Map<String, String> globalParameters = getRuntimeContext().getExecutionConfig()
          .getGlobalJobParameters().toMap();
      String redisHost = globalParameters.get(REDIS_HOST);
      String redisPort = globalParameters.get(REDIS_PORT);
      jedis = new Jedis(redisHost, Integer.parseInt(redisPort));
    }

    @Override
    public void asyncInvoke(Row input, final ResultFuture<Row> resultFuture) throws Exception {
      assert resultFuture instanceof StreamRecordQueueEntry;

      CompletableFuture.supplyAsync(() -> {
        String id = (String) input.getField(0);
        // 读取数据数据
        String parent = jedis.get(id);
        if (parent == null || parent.isEmpty()) {
          throw new DataNotFoundException("Student's parent can't find, id: " + id);
        }

        String[] parents = parent.split(",");
        Row output = new Row(input.getArity() + parents.length);
        int i = 0;
        for (; i < input.getArity(); ++i) {
          output.setField(i, input.getField(i));
        }
        for (int k = 0; i < output.getArity(); ++i) {
          output.setField(i, parents[k++]);
        }
        return output;
      }).whenComplete((Row output, Throwable cause) -> {
        if (cause != null) {
          resultFuture.completeExceptionally(cause);
        } else {
          resultFuture.complete(Collections.singleton(output));
        }
      });

    }

    @Override
    public void close() throws Exception {
      jedis.close();
    }
  }

  public static void main(String[] args) throws Exception {
    Jedis jedis = new Jedis("127.0.0.1", 6379);
    RedisUtils.addData(PARENTS, jedis);


    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // 全局参数
    env.getConfig().setGlobalJobParameters(new GlobalJobParameters() {
      @Override
      public Map<String, String> toMap() {
        Map<String, String> parameters = new HashMap<>();
        parameters.put(REDIS_HOST, "127.0.0.1");
        parameters.put(REDIS_PORT, "6379");
        return parameters;
      }
    });

    TypeInformation<?>[] columnTypes = new TypeInformation<?>[]{
        Types.STRING, Types.STRING, Types.STRING, Types.INT
    };
    String[] columnNames = new String[]{"id", "name", "sex", "age"};
    DataStream<Row> sourceStream = env.fromCollection(STUDENTS)
        .setParallelism(1)
        .returns(new RowTypeInfo(columnTypes, columnNames));

    /*
     * 1: 有序 / 无序队列
     *
     *    AsyncDataStream对数据流的有序 / 无序处理是指是否按照数据流入算子的顺序向下游算子发送数据.
     *
     *    引擎提供 UnorderedStreamElementQueue 和 OrderedStreamElementQueue 两种队列.
     *
     *    1.1 UnorderedStreamElementQueue
     *
     *       实现相对复杂, 内部使用双队列
     *
     *    1.2 OrderedStreamElementQueue
     *
     *       实现简单, 内部使用 ArrayDequeue. 当 StreamRecord 入对列时对 StreamRecordQueueEntry 添加回调函数, 该回调函数会唤醒
     *       Emitter线程
     *
     * 2: 数据状态(Task失败恢复)
     *
     *    使用 OperatorState, Task.open() 将 State 中的数据处理.
     *
     * 3: 数据处理逻辑(AsyncWaitOperator.processElement())
     *
     *   3.1 注册超时任务T
     *
     *       任务触发时间戳: 当前系统时间 + 超时时间.
     *
     *       若任务在超时前完成, 则会取消超时任务T; 若任务在超时前还为完成, UserFunction.timeout()触发
     *
     *   3.2 StreamRecord 消息入队列
     *
     *       StreamElementQueue.tryPut(): StreamRecord 添加到队列, 并对数据读取结果添加回调函数.
     *
     *       Emitter.run(): 读取 StreamElementQueue 队列数据, 若数据读取尚未完成, 则线程会被阻塞.
     *
     *   3.3 触发异步函数
     *
     *       AsyncFunction.asyncInvoke()对外部数据应异步读取, 读取完成后触发 StreamRecordQueueEntry.complete()
     *
     * **/
    DataStream<Row> resultStream = AsyncDataStream.orderedWait(sourceStream, new AsyncParentDataGetter(),
        3, TimeUnit.SECONDS, 1024);

    // 输出数据
    resultStream.addSink(new SinkFunction<Row>() {
      @Override
      public void invoke(Row value, Context context) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < value.getArity(); ++i) {
          if (i != 0) {
            sb.append(", ");
          }
          sb.append(value.getField(i));
        }
        sb.append("]");

        System.out.println("Row: " + sb.toString());
      }
    });

    env.execute("AsyncIOBootstrap");
  }

}
