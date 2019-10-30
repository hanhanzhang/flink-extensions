package com.sdu.blink.sql;

import com.sdu.flink.sink.SimplePrintAppendSink;
import com.sdu.flink.utils.SqlTypeUtils;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

/**
 * 流表关联
 *
 * @author hanhan.zhang
 * */
public class SqlDimTableJoinBootstrap {

	// 信息
	private static final Map<String, Row> DATA = new HashMap<>();

	// 成绩
	private static final List<Row> GRADES = new LinkedList<>();

	static {
		DATA.put("张小龙", Row.of("张小龙", 12, "男", "北京市朝阳区"));
		DATA.put("王彩霞", Row.of("王彩霞", 12, "女", "北京市海淀区"));
		DATA.put("李小虎", Row.of("李小虎", 11, "男", "济南市长清区"));
		DATA.put("马晓丽", Row.of("马晓丽", 13, "女", "济南市章丘区"));
		DATA.put("吕振涛", Row.of("吕振涛", 12, "男", "威海市环翠区"));

		GRADES.add(Row.of("张小龙", 81, 89));
		GRADES.add(Row.of("王彩霞", 87, 91));
		GRADES.add(Row.of("李小虎", 67, 71));
		GRADES.add(Row.of("马晓丽", 66, 78));
		GRADES.add(Row.of("吕振涛", 59, 65));
		GRADES.add(Row.of("石惊天", 56, 76));
	}

	public static class IdentityDataFetcher extends TableFunction<Row> {

		private Map<String, Row> identityData;

		IdentityDataFetcher(Map<String, Row> identityData) {
			this.identityData = identityData;
		}

		@Override
		public void open(FunctionContext context) throws Exception {
			//

		}

		public void eval(String identityId) {
			Row identity = identityData.get(identityId);
			if (identity == null) {
				// JOIN 失败不向下游发送数据(若是左连接, 会向下游发送数据, 缺失字段用 NULL 填充)
			} else {
				// JOIN 成功向下游发送数据
				collect(identity);
			}
		}

		@Override
		public boolean isDeterministic() {
			return true;
		}

		@Override
		public void close() throws Exception {
			//

		}
	}


	public static class AsyncIdentityDataFetcher extends AsyncTableFunction<Row> {

		private Map<String, Row> identityData;

		AsyncIdentityDataFetcher(Map<String, Row> identityData) {
			this.identityData = identityData;
		}

		@Override
		public void open(FunctionContext context) throws Exception {
			//
		}

		public void eval(final CompletableFuture<Row> result, String identityId) {
			result.complete(identityData.get(identityId));
		}

		@Override
		public boolean isDeterministic() {
			return true;
		}

		@Override
		public void close() throws Exception {
			//

		}
	}


	public static class IdentityTableSource implements LookupableTableSource<Row> {

		private final boolean isAsync;
		private final TableSchema tableSchema;

		IdentityTableSource(boolean isAsync, TableSchema tableSchema) {
			this.isAsync = isAsync;
			this.tableSchema = tableSchema;
		}

		@Override
		public TableFunction<Row> getLookupFunction(String[] lookupKeys) {
			return new IdentityDataFetcher(DATA);
		}

		@Override
		public AsyncTableFunction<Row> getAsyncLookupFunction(String[] lookupKeys) {
			return new AsyncIdentityDataFetcher(DATA);
		}

		@Override
		public boolean isAsyncEnabled() {
			return isAsync;
		}

		@Override
		public TableSchema getTableSchema() {
			return tableSchema;
		}

		@Override
		public DataType getProducedDataType() {
			return SqlTypeUtils.fromTableSchema(tableSchema);
		}
	}

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		EnvironmentSettings settings = EnvironmentSettings.newInstance()
				.inStreamingMode()
				.useBlinkPlanner()
				.build();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

		// 注册流表
		DataStream<Row> gradesStream = env.fromCollection(GRADES);
		Table gradesTable = tableEnv.fromDataStream(gradesStream, "name, math, chinese, ctime.proctime");
		tableEnv.registerTable("grades", gradesTable);

		// 流表关联仅支持LEFT JOIN、INNER JOIN、SEMI JOIN、ANTI JOIN, 即维表必须是右表
		// 注册维表
		TableSchema tableSchema = new TableSchema.Builder()
				.field("name", DataTypes.STRING())
				.field("age", DataTypes.INT())
				.field("sex", DataTypes.STRING())
				.field("address", DataTypes.STRING())
				.build();
		tableEnv.registerTableSource("useIdentity", new IdentityTableSource(false, tableSchema));

		// Temporal Table解读参考: https://cloud.tencent.com/developer/article/1417445
		// FOR SYSTEM_TIME AS OF声明, Calcite将语句用 'LogicalCorrelate', LogicalCorrelate.deriveRowType() 声明支持的JOIN类型
		// 优化规则:
		//   1: LogicalCorrelate  ==>> LogicalJoin [LogicalCorrelateToJoinFromTemporalTableRule]
		//   2: LogicalJoin       ==>> FlinkLogicalJoin [FlinkLogicalJoinConverter]
		//   3: FlinkLogicalJoin  ==>> StreamExecLookupJoin [BaseSnapshotOnTableScanRule]
		// StreamExecLookupJoin为Blink物理执行节点, 转为DataStream
		//
		String sqlText = "INSERT INTO user_result " +
				"SELECT a.name, age, sex, address, math, chinese FROM grades a " +
				"LEFT JOIN useIdentity FOR SYSTEM_TIME AS OF a.ctime AS b ON a.name = b.name";

		// 注册输出
		TableSchema sinkTableSchema = new TableSchema.Builder()
				.field("name", DataTypes.STRING())
				.field("age", DataTypes.INT())
				.field("sex", DataTypes.STRING())
				.field("address", DataTypes.STRING())
				.field("math", DataTypes.INT())
				.field("chinese", DataTypes.INT())
				.build();
		tableEnv.registerTableSink("user_result", new SimplePrintAppendSink(sinkTableSchema));

		tableEnv.sqlUpdate(sqlText);

		tableEnv.execute("SqlDimTableJoinBootstrap");

	}

}
