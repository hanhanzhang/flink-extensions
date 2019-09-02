package com.sdu.flink.sql;

import com.sdu.flink.sink.SimplePrintAppendSink;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author hanhan.zhang
 * */
public class SqlLateralTableBootstrap {

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

	public static class IdentityTableSource extends TableFunction<Row> {

		final Map<String, Row> identityData;

		IdentityTableSource(Map<String, Row> identityData) {
			this.identityData = identityData;
		}

		@Override
		public void open(FunctionContext context) throws Exception {
			// 初始化外部连接
		}

		public void eval(String name) {
			Row identity = identityData.get(name);
			if (identity != null) {
				collect(identity);
			}
		}

		@Override
		public void close() throws Exception {
			// 关闭外部连接
		}

		@Override
		public TypeInformation<Row> getResultType() {
			TypeInformation<?>[] columnTypes = new TypeInformation<?>[] {
					Types.STRING, Types.INT, Types.STRING, Types.STRING };
			String[] columnNames = new String[] {"name", "age", "sex", "address"};

			return new RowTypeInfo(columnTypes, columnNames);
		}

		@Override
		public boolean isDeterministic() {
			return true;
		}
	}

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		EnvironmentSettings settings = EnvironmentSettings.newInstance()
				.inStreamingMode()
				.useOldPlanner()
				.build();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

		tableEnv.registerFunction("userIdentity", new IdentityTableSource(DATA));

		TypeInformation<?>[] columnTypes = new TypeInformation<?>[] {
				Types.STRING, Types.INT, Types.INT };
		String[] columnNames = new String[] {"name", "math", "chinese"};
		DataStream<Row> scoreStream = env.fromCollection(GRADES)
				.returns(new RowTypeInfo(columnTypes, columnNames));

		tableEnv.registerDataStream("score", scoreStream);

		// Currently, only literal TRUE is supported as predicate for a left outer join against a lateral table
		// LATERAL TABLE: Calcite将其转为LogicalTableFunctionScan
		// FLINK转换规则:
		//  1: LogicalTableFunctionScan ---> FlinkLogicalTableFunctionScan[FlinkLogicalTableFunctionScanConverter]
		//  2: LogicalCorrelate         ---> FlinkLogicalCorrelate[FlinkLogicalCorrelateConverter]
		//  3: FlinkLogicalCorrelate    ---> DataStreamCorrelate[DataStreamCorrelateRule]
		String sqlText = "INSERT INTO user_result " +
				"SELECT a.name, sex, age, address, math, chinese FROM score a " +
				"LEFT JOIN LATERAL TABLE(userIdentity(name)) AS T(name, age, sex, address) ON TRUE";

		// 注册输出
		TableSchema sinkTableSchema = new TableSchema.Builder()
				.field("name", DataTypes.STRING())
				.field("sex", DataTypes.STRING())
				.field("age", DataTypes.INT())
				.field("address", DataTypes.STRING())
				.field("math", DataTypes.INT())
				.field("chinese", DataTypes.INT())
				.build();
		tableEnv.registerTableSink("user_result", new SimplePrintAppendSink(sinkTableSchema));

		tableEnv.sqlUpdate(sqlText);

		tableEnv.execute("SqlLateralTableBootstrap");

	}

}
